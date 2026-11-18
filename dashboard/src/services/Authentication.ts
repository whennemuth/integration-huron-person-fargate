import { APP_AUTHORIZATION_HEADER_NAME } from 'shibboleth-sp';
import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from '../handlers/DashboardTypes';
import { claimsReaderFromEnvironment } from '../authorization/ClaimsReader';

// The JWT cookie the edge functions set (AUTH_COOKIE_NAME, shared by sharedSignIn landscapes) is read by ClaimsReader
const { APP_LOGIN_HEADER, APP_LOGOUT_HEADER, APP_AUTHORIZATION='false' } = process.env;

/**
 * Handles authentication logic for incoming requests.
 */
export class Authenticator {
  
  constructor(private _event: LambdaFunctionUrlEvent) { }

  /**
   * The cloudfront origin request function will have already determined if the request 
   * presented a valid JWT token or not. This property reflects that determination.
   */
  public get isAuthenticated(): boolean {
    if( this.isPublicAccess ) {
      return true;
    }
    if(this.appWillAcceptAllPreAuthenticatedRequests) {
      return true;
    }
    return this._event.headers.authenticated == 'true';
  }

  /**
   * The application will decide on a case-by-case basis if any particular request
   * can be allowed despite not being pre-authenticated by CloudFront because it is for
   * a public resource. If false, all resources are considered protected and must be 
   * pre-authenticated.
   */
  public get appWillDecideAuth(): boolean {
    let appAuth = APP_AUTHORIZATION == 'true';
    if( ! appAuth) {
      appAuth = 'true' == this._event.headers?.[APP_AUTHORIZATION_HEADER_NAME.toLowerCase()];
    }
    return appAuth;
  }

  public get appWillAcceptAllPreAuthenticatedRequests(): boolean {
    return ! this.appWillDecideAuth;
  }
  
  /**
   * Access to the lambda is public if no login/logout headers are defined.
   */
  public get isPublicAccess(): boolean {
    if(APP_LOGIN_HEADER && APP_LOGOUT_HEADER) {
      return false;
    }
    return true;
  }


  /**
   * Get the login url to return for redirection to the IDP.
   * @param event
   * @returns 
   */
  public getLoginResponse = (): LambdaFunctionUrlResult => {
    const { headers } = this._event;
    const loginUrl = decodeURIComponent(headers[`${APP_LOGIN_HEADER}`.toLowerCase()]);
    const loginResponse = {
      statusCode: 302,
      // body: "login",
      headers: {
        ["content-type"]: "text/html",
        Location: loginUrl
      }
    }

    console.log(`LOGIN RESPONSE: ${JSON.stringify(loginResponse, null, 2)}`);
    return loginResponse;
  }
  
  /**
   * The signed-in user's claims, from a sign-in token whose signature checks out against JWT_PUBLIC_KEY
   * (ClaimsReader, authorization-plan.md decision 8), in the AUTH_COOKIE_NAME cookie; undefined without one. Never
   * decode the token without verifying it: roles depend on these claims.
   */
  public get user(): any {
    return claimsReaderFromEnvironment().claims(this._event.headers?.['cookie']);
  }
  public get event(): LambdaFunctionUrlEvent {
    return this._event;
  }
}