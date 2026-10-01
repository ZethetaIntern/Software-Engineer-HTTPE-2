import crypto from 'crypto';

/**
 * BFSI API Security Layer:
 * - API Key Validation
 * - HMAC-SHA256 Request Signature Verification
 * - Timestamp Skew Protection (anti-replay)
 */
export class AuthGateway {
  constructor(secretKey = 'zetheta-httpe-secret-prod-token') {
    this.secretKey = secretKey;
    this.allowedApiKeys = new Set([
      'api-key-fintech-partner-1',
      'api-key-merchant-portal-2',
      'api-key-mobile-gateway-3',
      'demo-key'
    ]);
  }

  validateRequest(headers, rawBody = '') {
    const apiKey = headers['x-api-key'];
    if (!apiKey) {
      return { valid: false, error: 'Missing X-API-KEY header.' };
    }

    if (!this.allowedApiKeys.has(apiKey)) {
      return { valid: false, error: 'Invalid or revoked API Key.' };
    }

    // Optional signature check if provided
    const signature = headers['x-signature'];
    const timestamp = headers['x-timestamp'];

    if (signature && timestamp) {
      const now = Date.now();
      const reqTime = parseInt(timestamp, 10);
      if (Math.abs(now - reqTime) > 300000) { // 5 minutes drift allowance
        return { valid: false, error: 'Request timestamp expired or skewed beyond 5 minutes.' };
      }

      const expectedSignature = crypto
        .createHmac('sha256', this.secretKey)
        .update(`${timestamp}.${typeof rawBody === 'string' ? rawBody : JSON.stringify(rawBody)}`)
        .digest('hex');

      if (signature !== expectedSignature) {
        return { valid: false, error: 'Invalid HMAC signature.' };
      }
    }

    return { valid: true };
  }
}
