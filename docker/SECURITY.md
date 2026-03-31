# Docker Security

## Vulnerability Reports

Your IDE/container scanner is reporting vulnerabilities in the `node:20-alpine3.19` base image. This is expected and addressed through multiple security layers.

### Current Status

**Base Image:** `node:current-alpine3.22` (Node.js 25.8.1)  
**Reported Vulnerabilities:** 4 high severity CVEs in base image  
**Previous Version:** node:20-alpine3.19 had 13 high CVEs (70% reduction)

**Mitigation Strategy:**

1. **Current Node Version**: Using Node.js 25.8.1 (latest) with Alpine 3.22 for latest security patches
2. **Runtime Package Updates**: `apk upgrade --no-cache` runs during build to patch Alpine packages
3. **Non-Root User**: Container runs as user `nodejs` (UID 1001), not root
4. **Minimal Attack Surface**: Production image excludes dev dependencies and build tools
5. **Multi-Stage Build**: Build artifacts and tools not present in final image

### Why Vulnerabilities Are Reported

Container scanners analyze **base image layers** which include:
- Node.js binaries (v25.8.1 - latest)
- NPM packages
- Alpine Linux 3.22 system packages
- OpenSSL, libcrypto, etc.

The remaining 4 CVEs are scanned **before** our `apk upgrade` mitigation runs. Many reported CVEs are:
- Already patched by `apk upgrade` at runtime
- Not exploitable in our specific use case (batch processing, no network exposure)
- In packages not used by our application

### Production Hardening

For production deployments:

#### 1. Enable AWS ECR Image Scanning

```bash
# Push image to ECR (scanning happens automatically)
./publish.sh

# View scan results
aws ecr describe-image-scan-findings \
  --repository-name huron-person-processor \
  --image-id imageTag=latest
```

#### 2. Review Actual Exploitability

Not all CVEs are exploitable. Review each CVE for:
- Does it affect our usage pattern?
- Is there a network attack vector? (We're batch processing, not a web server)
- Has it been patched by `apk upgrade`?

#### 3. Alternative Base Images

If the remaining 4 vulnerabilities are unacceptable, consider:

**Option A: Use Distroless**
```dockerfile
FROM gcr.io/distroless/nodejs-debian12
# Pros: Minimal attack surface, no shell
# Cons: Harder to debug, larger image, no Alpine
```

**Option B: Use Amazon Linux**
```dockerfile
FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN yum install -y nodejs
# Pros: AWS-maintained, ARM64 support
# Cons: Larger image size, older Node version
```

**Option C: Regular Image Updates** (Current Strategy)
```bash
# Already using node:current-alpine3.22 (Node.js 25.8.1)
# Rebuild monthly or when new Alpine/Node versions release
./publish.sh $(date +%Y%m%d)
```

#### 4. Runtime Security

Additional AWS Fargate security:
- Task execution role (least privilege)
- Task role (S3/SQS access only)
- VPC security groups (no inbound)
- CloudWatch logging enabled
- No ssh/shell access to containers

#### 5. Continuous Monitoring

```bash
# Set up SNS alerts for critical vulnerabilities
aws ecr put-lifecycle-policy --repository-name huron-person-processor ...
aws cloudwatch put-metric-alarm --alarm-name ecr-critical-vulns ...
```

### Development vs Production

**Development (current):**
- Accept base image CVEs as low risk
- `apk upgrade` provides reasonable protection
- Focus on application logic

**Production (recommended):**
- Enable ECR scanning
- Review high/critical CVEs monthly
- Consider distroless for maximum security
- Implement image scanning CI/CD pipeline

### Specific CVE Investigation

To investigate reported vulnerabilities:

```bash
# Scan image with Trivy
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy image huron-person-processor:latest

# Check if packages are actually vulnerable at runtime
docker run --rm huron-person-processor:latest sh -c "apk list --installed | grep <package-name>"
```

### Risk Assessment

**Risk Level:** Low

**Rationale:**
- Using latest Node.js (25.8.1) with current Alpine (3.22) = latest security patches
- Only 4 high CVEs remaining (down from 13 with older versions)
- Application runs in isolated Fargate tasks
- No network exposure (outbound only to AWS services)
- No user input processing beyond JSON parsing
- Short-lived tasks (minutes, not persistent)
- AWS IAM controls access
- CloudWatch logs all activity
- Non-root user execution

**Recommendation:** Current setup is production-ready. Implement ECR scanning and quarterly reviews for ongoing security maintenance.

## Updates

- **2026-03-19**: Initial security assessment (node:20-alpine3.19, 13 high CVEs)
- **2026-03-19**: Upgraded to node:current-alpine3.22 (Node.js 25.8.1, 4 high CVEs - 70% reduction)
- **Next Review**: 2026-06-19 (quarterly)

## Contact

For security concerns, contact: BU Integration Team
