import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from "../handlers/DashboardTypes";
import { OriginHeader } from "../handlers/OriginVerification";
import { Authenticator } from "./Authentication";

/**
 * Encapsulates request authentication and authorization logic.
 */
export class Request {
  private event: LambdaFunctionUrlEvent;
  
  constructor(private authenticator: Authenticator, private originHeader?: OriginHeader) { 
    this.event = authenticator.event;
  }

  public get isAuthenticated(): boolean {
    return this.authenticator.isAuthenticated;
  }
  public get path (): string {
    return this.event.rawPath || this.event.requestContext?.http?.path || '/';
  }
  public isAuthorized = (): boolean => {
    return this.authenticator.isAuthorized();
  }
  public getLoginResponse = (): LambdaFunctionUrlResult => {
    return this.authenticator.getLoginResponse();
  }
  public get user(): any {
    return this.authenticator.user;
  }
  public get queryParams(): { [key: string]: string } {
    return this.event.queryStringParameters || {};
  }
  public get method(): string {
    return this.event.requestContext?.http?.method || 'GET';
  }
  public get body(): string | null {
    return this.event.body ? (this.event.isBase64Encoded ? Buffer.from(this.event.body, 'base64').toString() : this.event.body) : null;
  }
  public getOriginVerifiedErrorResponse = (): LambdaFunctionUrlResult | undefined => {
    if(this.originHeader) {
      const result = this.originHeader.getVerificationResult();
      if (!result.isValid && result.errorResponse) {
        return result.errorResponse;
      }
    }
    return undefined;
  }
  public get originVerified(): boolean {
    const errorResponse = this.getOriginVerifiedErrorResponse();
    if(errorResponse) {
      return false;
    }
    return true;
  }
}