---
name: Bug report
about: Report a problem with the operator
labels: bug
---

**What happened?**
What you saw, and what you expected instead.

**Environment**
- Operator version:
- Kubernetes version:
- Install method: <!-- Helm / kustomize -->

**Reproduction**
The `Superset` CR (redact secrets) and relevant operator logs:

```
kubectl logs deploy/superset-operator-controller-manager -n superset-operator-system
```
