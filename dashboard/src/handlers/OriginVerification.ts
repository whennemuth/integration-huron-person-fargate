/**
 * Origin verification utility for validating CloudFront requests
 */

import { isRunningInLambda } from '../Utils';
import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from './DashboardTypes';

export interface OriginVerificationResult {
  isValid: boolean;
  errorResponse?: LambdaFunctionUrlResult;
}

/**
 * Class for verifying origin headers from CloudFront requests
 */
export class OriginHeader {
  private verificationResult: OriginVerificationResult;

  constructor(private _event: LambdaFunctionUrlEvent) {
    this.verificationResult = this.computeVerification();
  }

  /**
   * Returns true if the origin header is verified, false otherwise
   */
  public isVerified(): boolean {
    return this.verificationResult.isValid;
  }

  /**
   * Returns the full verification result
   */
  public getVerificationResult(): OriginVerificationResult {
    return this.verificationResult;
  }

  /**
   * Computes the verification result based on the event
   */
  private computeVerification(): OriginVerificationResult {
    const expectedSecret = process.env.ORIGIN_VERIFY_SECRET;

    if (!isRunningInLambda()) {
      // If not Lambda environment, skip verification (might be running locally or in a test environment).
      return { isValid: true };
    }

    if (!expectedSecret) {
      console.error('ORIGIN_VERIFY_SECRET environment variable not set');
      return {
        isValid: false,
        errorResponse: {
          statusCode: 500,
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            error: 'Server configuration error',
            message: 'Origin verification not configured'
          }),
        }
      };
    }

    const originVerifyHeader = this._event.headers['X-Origin-Verify'] || this._event.headers['x-origin-verify'];

    if (!originVerifyHeader) {
      console.warn('Request blocked: Missing X-Origin-Verify header', {
        sourceIp: this._event.requestContext?.http?.sourceIp,
        userAgent: this._event.headers['User-Agent'] || 'Unknown',
        path: this._event.rawPath
      });

      return {
        isValid: false,
        errorResponse: {
          statusCode: 403,
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            error: 'Access Denied',
            message: 'Direct access not allowed'
          }),
        }
      };
    }

    if (originVerifyHeader !== expectedSecret) {
      console.warn('Request blocked: Invalid X-Origin-Verify header', {
        sourceIp: this._event.requestContext?.http?.sourceIp,
        userAgent: this._event.headers['User-Agent'] || 'Unknown',
        path: this._event.rawPath,
        headerValue: originVerifyHeader.substring(0, 10) + '...' // Log partial value for debugging
      });

      return {
        isValid: false,
        errorResponse: {
          statusCode: 403,
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            error: 'Access Denied',
            message: 'Invalid origin verification'
          }),
        }
      };
    }

    return { isValid: true };
  }

  public get event(): LambdaFunctionUrlEvent {
    return this._event;
  }
}
