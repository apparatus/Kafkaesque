# Changelog

1.0.0 - 2 August 2016:
---

- Doing offset commits and fetches to kafka, not zookeeper
- Group membership api implemented. Aimed compatible with java and scala clients
- Much more of the low-level binary api is implemented
- Added `connect` and `disconnect` methods, now the prefered manner of connecting to cluster when managing partitions
