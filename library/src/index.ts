/**
 * ShadowCast - P2P WebRTC Asset Delivery Client
 * Vanilla TS, zero heavy deps.
 */

interface Chunk {
  index: number;
  total: number;
  assetId: string;
  data: string;
}

interface ChunkBuffer {
  received: Map<number, ArrayBuffer>;
  total: number;
  mimeType: string;
}

interface PendingAsset {
  url: string;
  resolve: (url: string) => void;
}

const CHUNK_SIZE = 64 * 1024;
const DEFAULT_ROOM = 'shadowcast-global';

export class ShadowCast {
  private peerId: string = crypto.randomUUID();

  private peers: Map<string, RTCPeerConnection> = new Map();
  private dataChannels: Map<string, RTCDataChannel> = new Map();
  private signalingWs: WebSocket | null = null;
  private assetCache: Map<string, Blob> = new Map();
  private roomId: string = '';
  private initialized = false;
  private connectingPromise: Promise<void> | null = null;

  private chunkBuffers: Map<string, ChunkBuffer> = new Map();
  private assetResolvers: Map<string, PendingAsset> = new Map();
  private assetUrlsById: Map<string, string> = new Map();
  private pendingChunkMeta: Map<string, { assetId: string; index: number }> = new Map();
  private peerWaiters: Set<() => void> = new Set();

  constructor(private signalingUrl: string = 'ws://localhost:8080/ws') {}

  async init(roomId: string = DEFAULT_ROOM): Promise<void> {
    return this.connect(roomId);
  }

  async connect(roomId: string = DEFAULT_ROOM): Promise<void> {
    if (this.initialized && this.roomId === roomId && this.signalingWs?.readyState === WebSocket.OPEN) {
      return;
    }

    if (this.connectingPromise && this.roomId === roomId) {
      return this.connectingPromise;
    }

    if (this.signalingWs || this.peers.size > 0 || this.dataChannels.size > 0) {
      this.cleanupMesh();
    }

    this.roomId = roomId;
    this.initialized = false;

    this.connectingPromise = new Promise((resolve, reject) => {
      const ws = new WebSocket(this.signalingUrl);
      this.signalingWs = ws;

      ws.onopen = () => {
        ws.send(JSON.stringify({ type: 'join', room: roomId, from: this.peerId }));
        this.initialized = true;
        this.connectingPromise = null;
        resolve();
      };

      ws.onmessage = async (event) => {
        try {
          const msg = JSON.parse(event.data as string);
          await this.handleSignalingMessage(msg);
        } catch (err) {
          console.error('[ShadowCast] signaling message error:', err);
        }
      };

      ws.onerror = (err) => {
        this.connectingPromise = null;
        reject(err);
      };

      ws.onclose = () => {
        this.initialized = false;
        this.connectingPromise = null;
      };
    });

    return this.connectingPromise;
  }

  waitForPeers({ timeout = 5000 }: { timeout?: number } = {}): Promise<boolean> {
    if (this.hasOpenDataChannel()) {
      return Promise.resolve(true);
    }

    return new Promise((resolve) => {
      const done = (ready: boolean) => {
        clearTimeout(timer);
        this.peerWaiters.delete(onPeerReady);
        resolve(ready);
      };

      const onPeerReady = () => done(true);
      const timer = setTimeout(() => done(false), timeout);
      this.peerWaiters.add(onPeerReady);
    });
  }

  private async handleSignalingMessage(msg: Record<string, unknown>) {
    switch (msg.type) {
      case 'self-id':
        this.peerId = msg.id as string;
        break;

      case 'room-peers': {
        const peers = msg.peers as string[];
        for (const peerId of peers) {
          await this.maybeInitiateOffer(peerId);
        }
        break;
      }

      case 'peer-joined':
        await this.maybeInitiateOffer(msg.id as string);
        break;

      case 'peer-left':
        this.handlePeerDisconnect(msg.id as string);
        break;

      case 'offer':
        await this.handleOffer(msg);
        break;

      case 'answer':
        await this.handleAnswer(msg);
        break;

      case 'ice-candidate':
        await this.handleIceCandidate(msg);
        break;
    }
  }

  private shouldInitiate(remotePeerId: string): boolean {
    return this.peerId < remotePeerId;
  }

  private async maybeInitiateOffer(peerId: string): Promise<void> {
    if (!peerId || peerId === this.peerId || !this.shouldInitiate(peerId) || this.peers.has(peerId)) {
      return;
    }
    await this.initiateOffer(peerId);
  }

  private createPeer(peerId: string): RTCPeerConnection {
    const existing = this.peers.get(peerId);
    if (existing && existing.signalingState !== 'closed') {
      return existing;
    }

    const pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
      ],
    });

    pc.onicecandidate = (event) => {
      if (event.candidate && this.signalingWs?.readyState === WebSocket.OPEN) {
        this.signalingWs.send(
          JSON.stringify({
            type: 'ice-candidate',
            target: peerId,
            from: this.peerId,
            candidate: event.candidate,
          })
        );
      }
    };

    pc.onconnectionstatechange = () => {
      if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) {
        this.handlePeerDisconnect(peerId);
      }
    };

    pc.ondatachannel = (event) => {
      this.setupDataChannel(event.channel, peerId);
    };

    this.peers.set(peerId, pc);
    return pc;
  }

  private async initiateOffer(peerId: string): Promise<void> {
    const pc = this.createPeer(peerId);

    if (!this.dataChannels.has(peerId)) {
      const dc = pc.createDataChannel('asset-transfer');
      this.setupDataChannel(dc, peerId);
    }

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    this.signalingWs?.send(
      JSON.stringify({
        type: 'offer',
        target: peerId,
        from: this.peerId,
        sdp: pc.localDescription,
      })
    );
  }

  private async handleOffer(msg: Record<string, unknown>): Promise<void> {
    const fromId = msg.from as string;
    if (!fromId || fromId === this.peerId) return;

    const existing = this.peers.get(fromId);
    if (existing?.remoteDescription && existing.signalingState === 'stable') {
      return;
    }

    if (existing && existing.signalingState !== 'stable' && this.shouldInitiate(fromId)) {
      return;
    }

    const pc = this.createPeer(fromId);
    await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp as RTCSessionDescriptionInit));

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    this.signalingWs?.send(
      JSON.stringify({
        type: 'answer',
        target: fromId,
        from: this.peerId,
        sdp: pc.localDescription,
      })
    );
  }

  private async handleAnswer(msg: Record<string, unknown>): Promise<void> {
    const pc = this.peers.get(msg.from as string);
    if (!pc || pc.signalingState === 'stable') return;
    await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp as RTCSessionDescriptionInit));
  }

  private async handleIceCandidate(msg: Record<string, unknown>): Promise<void> {
    const pc = this.peers.get(msg.from as string);
    if (!pc || !msg.candidate) return;
    await pc.addIceCandidate(new RTCIceCandidate(msg.candidate as RTCIceCandidateInit));
  }

  private setupDataChannel(dc: RTCDataChannel, peerId: string) {
    dc.binaryType = 'arraybuffer';

    dc.onopen = () => {
      this.dataChannels.set(peerId, dc);
      this.notifyPeerWaiters();
    };

    dc.onmessage = (event) => {
      this.handleIncomingData(event.data as ArrayBuffer | string, peerId);
    };

    dc.onclose = () => {
      this.dataChannels.delete(peerId);
    };

    dc.onerror = (err) => {
      console.error(`[ShadowCast] DataChannel error with ${peerId}:`, err);
    };
  }

  async requestAsset(url: string): Promise<string> {
    const cached = this.assetCache.get(url);
    if (cached) return URL.createObjectURL(cached);

    await this.connect(this.roomId || DEFAULT_ROOM);

    const assetId = this.getAssetId(url);
    this.assetUrlsById.set(assetId, url);

    if (await this.waitForPeers({ timeout: 1500 })) {
      return new Promise<string>((resolve) => {
        this.assetResolvers.set(assetId, { url, resolve });
        this.requestFromPeers(assetId, url);

        setTimeout(() => {
          if (this.assetResolvers.has(assetId)) {
            this.assetResolvers.delete(assetId);
            this.fallbackToHttp(url).then(resolve);
          }
        }, 5000);
      });
    }

    return this.fallbackToHttp(url);
  }

  private requestFromPeers(assetId: string, url: string) {
    const req = JSON.stringify({ type: 'asset-request', assetId, url });
    for (const dc of this.dataChannels.values()) {
      if (dc.readyState === 'open') {
        dc.send(req);
      }
    }
  }

  private async sendAssetChunks(dc: RTCDataChannel, assetId: string, blob: Blob): Promise<void> {
    const totalChunks = Math.ceil(blob.size / CHUNK_SIZE);

    for (let i = 0; i < totalChunks; i++) {
      const slice = blob.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
      const buffer = await slice.arrayBuffer();
      const meta: Chunk = {
        index: i,
        total: totalChunks,
        assetId,
        data: '',
      };

      if (dc.readyState !== 'open') break;
      dc.send(JSON.stringify({ ...meta, type: 'chunk-meta' }));
      dc.send(buffer);
    }

    if (dc.readyState === 'open') {
      dc.send(JSON.stringify({ type: 'chunk-done', assetId, mimeType: blob.type }));
    }
  }

  private handleIncomingData(data: ArrayBuffer | string, peerId: string) {
    if (typeof data === 'string') {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(data);
      } catch {
        return;
      }

      switch (msg.type) {
        case 'asset-request': {
          const { assetId, url } = msg as { assetId: string; url: string };
          const blob = this.assetCache.get(url);
          const dc = this.dataChannels.get(peerId);
          if (blob && dc?.readyState === 'open') {
            this.sendAssetChunks(dc, assetId, blob);
          }
          break;
        }

        case 'chunk-meta': {
          const { assetId, index, total } = msg as unknown as Chunk & { type: string };
          this.assetUrlsById.set(assetId, this.assetUrlsById.get(assetId) ?? assetId);
          if (!this.chunkBuffers.has(assetId)) {
            this.chunkBuffers.set(assetId, {
              received: new Map(),
              total,
              mimeType: 'application/octet-stream',
            });
          }
          this.pendingChunkMeta.set(peerId, { assetId, index });
          break;
        }

        case 'chunk-done': {
          const { assetId, mimeType } = msg as { assetId: string; mimeType: string };
          const buf = this.chunkBuffers.get(assetId);
          if (buf) {
            buf.mimeType = mimeType;
            this.tryReassemble(assetId);
          }
          break;
        }
      }
      return;
    }

    this.storeBinaryChunk(data, peerId);
  }

  private storeBinaryChunk(buffer: ArrayBuffer, peerId: string) {
    const meta = this.pendingChunkMeta.get(peerId);
    if (!meta) return;

    const buf = this.chunkBuffers.get(meta.assetId);
    if (!buf) return;

    buf.received.set(meta.index, buffer);
    this.pendingChunkMeta.delete(peerId);
    this.tryReassemble(meta.assetId);
  }

  private tryReassemble(assetId: string) {
    const buf = this.chunkBuffers.get(assetId);
    if (!buf || buf.received.size < buf.total) return;

    const parts: ArrayBuffer[] = [];
    for (let i = 0; i < buf.total; i++) {
      const chunk = buf.received.get(i);
      if (!chunk) return;
      parts.push(chunk);
    }

    const blob = new Blob(parts, { type: buf.mimeType });
    const pending = this.assetResolvers.get(assetId);
    const url = pending?.url ?? this.assetUrlsById.get(assetId);

    if (url) {
      this.assetCache.set(url, blob);
    }

    this.chunkBuffers.delete(assetId);

    if (pending) {
      this.assetResolvers.delete(assetId);
      pending.resolve(URL.createObjectURL(blob));
    }
  }

  private async fallbackToHttp(url: string): Promise<string> {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`[ShadowCast] HTTP fallback failed: ${response.status} ${url}`);
    }
    const blob = await response.blob();
    this.assetCache.set(url, blob);
    return URL.createObjectURL(blob);
  }

  private handlePeerDisconnect(peerId: string) {
    const dc = this.dataChannels.get(peerId);
    if (dc && dc.readyState !== 'closed') {
      dc.close();
    }
    this.dataChannels.delete(peerId);

    const pc = this.peers.get(peerId);
    if (pc && pc.signalingState !== 'closed') {
      pc.close();
    }
    this.peers.delete(peerId);
    this.pendingChunkMeta.delete(peerId);
  }

  private cleanupMesh() {
    const ws = this.signalingWs;
    this.signalingWs = null;
    ws?.close();

    for (const peerId of this.peers.keys()) {
      this.handlePeerDisconnect(peerId);
    }

    this.peers.clear();
    this.dataChannels.clear();
    this.pendingChunkMeta.clear();
    this.initialized = false;
  }

  private hasOpenDataChannel(): boolean {
    for (const dc of this.dataChannels.values()) {
      if (dc.readyState === 'open') return true;
    }
    return false;
  }

  private notifyPeerWaiters() {
    if (!this.hasOpenDataChannel()) return;
    for (const waiter of this.peerWaiters) {
      waiter();
    }
    this.peerWaiters.clear();
  }

  private getAssetId(url: string): string {
    return btoa(url).replace(/[^a-zA-Z0-9]/g, '').slice(0, 20);
  }

  destroy() {
    this.cleanupMesh();
    this.chunkBuffers.clear();
    this.assetResolvers.clear();
    this.assetUrlsById.clear();
    this.peerWaiters.clear();
  }
}
