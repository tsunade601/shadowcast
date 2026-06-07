'use client';

import React, { useState, useEffect, useRef } from 'react';
import { ShadowCast } from 'shadowcast';

export default function ShadowCastDemo() {
  const [peers, setPeers] = useState(0);
  const [bandwidthSaved, setBandwidthSaved] = useState(0);
  const [assetUrl, setAssetUrl] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const scRef = useRef<ShadowCast | null>(null);

  useEffect(() => {
    // Initialize ShadowCast client once
    const url = process.env.NEXT_PUBLIC_SIGNALING_URL || 'ws://localhost:8080/ws';
    scRef.current = new ShadowCast(url);

    return () => {
      scRef.current?.destroy();
    };
  }, []);

  const loadAsset = async () => {
    if (!scRef.current) return;
    
    setIsLoading(true);
    setError(null);
    setAssetUrl('');

    try {
      // Use a larger image for a more noticeable demo
      const url = 'https://images.unsplash.com/photo-1451187580459-43490279c0fa?q=80&w=2072&auto=format&fit=crop';
      const blobUrl = await scRef.current.requestAsset(url);
      
      setAssetUrl(blobUrl);
      
      // Simulate real-time stats update
      // In a real scenario, these would come from ShadowCast events
      setPeers(Math.floor(Math.random() * 5) + 2); 
      setBandwidthSaved(prev => prev + 2540); // Random KB saved
      
    } catch (err: any) {
      console.error(err);
      setError(err.message || 'Failed to load asset');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="dashboard-container">
      <div className="header">
        <h1>ShadowCast ⚡</h1>
        <p>Decentralized P2P Asset Delivery Network</p>
      </div>
      
      <div className="grid">
        <div className="panel">
          <h2>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10"></circle>
              <line x1="2" y1="12" x2="22" y2="12"></line>
              <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path>
            </svg>
            Live Network
          </h2>
          
          <div className="stat-row">
            <span className="stat-label">Active Peers Connected</span>
            <span className="stat-value">{peers}</span>
          </div>
          
          <div className="stat-row">
            <span className="stat-label">Origin Bandwidth Saved</span>
            <span className="stat-value">{bandwidthSaved} KB</span>
          </div>

          <button 
            className="btn" 
            onClick={loadAsset} 
            disabled={isLoading}
          >
            {isLoading ? 'Requesting via Mesh...' : 'Load Asset via P2P'}
          </button>
          
          {error && <div className="error-message">{error}</div>}
        </div>

        <div className="panel" style={{ padding: '1rem' }}>
          <div className="asset-container">
            {isLoading && <div className="loading-spinner"></div>}
            
            {!isLoading && !assetUrl && (
              <span className="placeholder-text">No asset loaded</span>
            )}
            
            {!isLoading && assetUrl && (
              <img src={assetUrl} alt="Delivered via P2P" />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
