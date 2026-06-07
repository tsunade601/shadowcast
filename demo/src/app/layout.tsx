import './globals.css';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'ShadowCast Demo',
  description: 'P2P WebRTC asset delivery network',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
