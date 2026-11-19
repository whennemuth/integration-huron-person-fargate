/**
 * Who is making a request (authorization-plan.md, decision 2): the person's key is their `buPrincipal`, the first
 * value of the sign-in token's claim. The IdP sends every claim as an array (`"buPrincipal": ["wrh"]`, see
 * `scrap/claims.json`); a plain string is accepted too.
 */
export class Principal {
  private constructor(
    /** The person's key in role assignments and the audit trail; undefined for an anonymous principal */
    readonly buPrincipal: string | undefined,
    readonly eppn?: string,
    readonly mail?: string,
    readonly displayName?: string
  ) {}

  /** The principal of the sign-in token's user claims; anonymous when there is no usable `buPrincipal` */
  static fromClaims(claims: unknown): Principal {
    if (!claims || typeof claims !== 'object') {
      return Principal.anonymous();
    }
    const c = claims as Record<string, unknown>;
    const buPrincipal = normalizeBuPrincipal(firstValue(c.buPrincipal));
    if (!buPrincipal) {
      return Principal.anonymous();
    }
    const given = firstValue(c.eduPersonNickname) ?? firstValue(c.givenName);
    const surname = firstValue(c.sn);
    const displayName = [given, surname].filter(Boolean).join(' ') || undefined;
    return new Principal(buPrincipal, firstValue(c.eduPersonPrincipalName), firstValue(c.mail), displayName);
  }

  /** No one signed in: a public dashboard, or a request without a usable sign-in token */
  static anonymous(): Principal {
    return new Principal(undefined);
  }

  /** A principal known only by its key (a person chosen on the Access page, or to impersonate) */
  static of(buPrincipal: string): Principal {
    const key = normalizeBuPrincipal(buPrincipal);
    if (!key) {
      throw new Error(`Not a buPrincipal: ${JSON.stringify(buPrincipal)}`);
    }
    return new Principal(key);
  }

  get isAnonymous(): boolean {
    return this.buPrincipal === undefined;
  }

  /** How the dashboard names the person: their name, else their eppn, else their buPrincipal */
  get label(): string {
    return this.displayName ?? this.eppn ?? this.buPrincipal ?? 'anonymous';
  }
}

export type PrincipalResolution =
  | { ok: true; buPrincipal: string; from: 'buPrincipal' | 'eppn' }
  | { ok: false; reason: string };

/**
 * Turns what an Admin types (on the Access page, or to impersonate) into a `buPrincipal` (resolved point 1): a
 * `buPrincipal` as it is, or an `eduPersonPrincipalName`, accepted only as `<id>@bu.edu`, whose `<id>` is the
 * `buPrincipal`.
 */
export class PrincipalResolver {
  static readonly EPPN_DOMAIN = 'bu.edu';
  /** A BU login name: letters, digits and the few separators logins use */
  static readonly BU_PRINCIPAL_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;

  static resolve(input: unknown): PrincipalResolution {
    if (typeof input !== 'string' || !input.trim()) {
      return { ok: false, reason: 'Enter a buPrincipal or an eduPersonPrincipalName' };
    }
    const value = input.trim().toLowerCase();
    if (value.includes('@')) {
      const [id, domain, ...rest] = value.split('@');
      if (rest.length || domain !== PrincipalResolver.EPPN_DOMAIN) {
        return { ok: false, reason: `An eduPersonPrincipalName must end with @${PrincipalResolver.EPPN_DOMAIN}` };
      }
      return PrincipalResolver.check(id, 'eppn');
    }
    return PrincipalResolver.check(value, 'buPrincipal');
  }

  private static check(id: string, from: 'buPrincipal' | 'eppn'): PrincipalResolution {
    return PrincipalResolver.BU_PRINCIPAL_PATTERN.test(id)
      ? { ok: true, buPrincipal: id, from }
      : { ok: false, reason: `Not a valid buPrincipal: ${id || '(empty)'}` };
  }
}

/** A claim's first value, trimmed; undefined when it has none */
const firstValue = (claim: unknown): string | undefined => {
  const value = Array.isArray(claim) ? claim[0] : claim;
  if (typeof value !== 'string') {
    return undefined;
  }
  return value.trim() || undefined;
};

/** buPrincipals are compared lower-cased, so an assignment typed in another case still matches the token's */
const normalizeBuPrincipal = (value: string | undefined): string | undefined => value?.trim().toLowerCase() || undefined;
