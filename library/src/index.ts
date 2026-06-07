/**
 * ShadowCast - P2P WebRTC Asset Delivery Client
 * Vanilla TS, zero heavy deps.
 */

import { v4 as uuidv4 } from 'uuid';

interface Chunk {
  index: number;
  total: number;
  data: ArrayBuffer;
  hash?: string;
}

export class ShadowCast {
  private peers: Map<string, RTCPeerConnection> = new Map();
  private dataChannels: Map<string, RTCDataChannel> = new Map();
  private signalingWs: WebSocket | null = null;
  private assetCache: Map<string, Blob> = new Map();
  private roomId: string = '';

  constructor(private signalingUrl: string = 'ws://localhost:8080') {}

  async init(roomId: string) {
    this.roomId = roomId;
    this.signalingWs = new WebSocket(this.signalingUrl);
    
    this.signalingWs.onmessage = async (event) => {
      const msg = JSON.parse(event.data);
      await this.handleSignalingMessage(msg);
    };

    this.signalingWs.onopen = () => {
      this.signalingWs?.send(JSON.stringify({ type: 'join', room: roomId }));
    };
  }

  private async handleSignalingMessage(msg: any) {
    if (msg.type === 'offer') {
      await this.handleOffer(msg);
    } else if (msg.type === 'answer') {
      await this.handleAnswer(msg);
    } else if (msg.type === 'ice-candidate') {
      await this.handleIceCandidate(msg);
    }
  }

  private async createPeer(peerId: string): Promise<RTCPeerConnection> {
    const pc = new RTCPeerConnection({
      iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
    });

    pc.onicecandidate = (event) => {
      if (event.candidate && this.signalingWs) {
        this.signalingWs.send(JSON.stringify({
          type: 'ice-candidate',
          target: peerId,
          candidate: event.candidate
        }));
      }
    };

    const dc = pc.createDataChannel('asset-transfer');
    this.setupDataChannel(dc, peerId);

    this.peers.set(peerId, pc);
    this.dataChannels.set(peerId, dc);

    return pc;
  }

  private setupDataChannel(dc: RTCDataChannel, peerId: string) {
    dc.onopen = () => console.log(`DataChannel open with ${peerId}`);
    dc.onmessage = (event) => this.handleChunk(event.data, peerId);
    dc.onclose = () => this.handlePeerDisconnect(peerId);
  }

  async requestAsset(url: string): Promise<string> {
    const cached = this.assetCache.get(url);
    if (cached) return URL.createObjectURL(cached);

    // Join signaling for this asset
    const assetId = btoa(url).slice(0, 20);
    await this.init(assetId);

    // TODO: Full P2P logic
    return this.fallbackToHttp(url);
  }

  private fallbackToHttp(url: string): string {
    // In real use: fetch and return blob URL
    console.log('Falling back to HTTP for', url);
    return url;
  }

  private handleChunk(data: ArrayBuffer, peerId: string) {
    console.log('Received chunk from', peerId, data.byteLength);
    // TODO: Implement full reassembly
  }

  private handlePeerDisconnect(peerId: string) {
    console.log(`Peer disconnected: ${peerId}. Falling back to HTTP.`);
    this.peers.delete(peerId);
    this.dataChannels.delete(peerId);
  }

  // TODO: Add offer/answer/ICE handlers, chunking logic, Service Worker integration
}
