'use client';

import React, { useState, useEffect } from 'react';

export default function ShadowCastDemo() {
  const [peers, setPeers] = useState(0);
  const [bandwidthSaved, setBandwidthSaved] = useState(0);
  const [assetUrl, setAssetUrl] = useState('');

  const loadAsset = async () => {
    // Simulate P2P load
    const sc = new (window as any).ShadowCast();
    const blobUrl = await sc.requestAsset('https://picsum.photos/id/1015/1200/800');
    setAssetUrl(blobUrl);
    setPeers(3);
    setBandwidthSaved(12400);
  };

  return (
    <div style={{
      minHeight: '100vh',
      background: '#0a0a0a',
      color: '#fff',
      fontFamily: 'system-ui, sans-serif',
      padding: '2rem'
    }}>
      <h1 style={{ textAlign: 'center', marginBottom: '2rem' }}>ShadowCast ⚡</h1>
      
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
        <div>
          <h2>Live Network</h2>
          <div style={{ background: '#1a1a1a', padding: '1.5rem', borderRadius: '12px' }}>
            <p>Active Peers: <strong>{peers}</strong></p>
            <p>Bandwidth Saved: <strong>{bandwidthSaved} KB</strong></p>
            <button onClick={loadAsset} style={{ padding: '0.75rem 1.5rem', background: '#00ff9f', color: '#000', border: 'none', borderRadius: '8px', cursor: 'pointer', marginTop: '1rem' }}>
              Load Asset via P2P Mesh
            </button>
          </div>
        </div>

        <div>
          {assetUrl && <img src={assetUrl} alt="P2P Asset" style={{ maxWidth: '100%', borderRadius: '12px' }} />}
        </div>
      </div>
    </div>
  );
}
