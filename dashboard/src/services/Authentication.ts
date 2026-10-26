import { APP_AUTHORIZATION_HEADER_NAME, JwtTools } from 'shibboleth-sp';
import { LambdaFunctionUrlEvent, LambdaFunctionUrlResult } from '../handlers/DashboardTypes';

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

  public isAuthorized = (): boolean => {
    // TODO: implement real authorization check
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
   * The "cookie" header of the request may contain a jwt with user data inside. Extract the user data.
   * @param cookies 
   * @param cookieName 
   * @returns 
   */
  public getUserFromJwt = (cookies:string, cookieName:string):string|undefined => {
    try {
      const authTokenJson = atob(
        cookies
          .split(/;\x20?/)
          .filter(a => a.split('=')[0] == cookieName)
          .map(c => c.split('.')[1])[0]
      );
      return (JSON.parse(authTokenJson))[JwtTools.TOKEN_NAME].user
    }
    catch(e) {
      return undefined;
    }
  }

  public get user(): any {
    return this.getUserFromJwt(this._event.headers['cookie'], JwtTools.COOKIE_NAME);
  }
  public get event(): LambdaFunctionUrlEvent {
    return this._event;
  }
}