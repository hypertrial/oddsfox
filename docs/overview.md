# Overview

**oddsfox** v0.1.0 answers five research questions locally:

1. What markets exist?
2. What were their probabilities over time?
3. How liquid were they?
4. How did they resolve?
5. How accurate/calibrated were they?

## Non-goals

- Trading, signing, wallets
- Hosted data mirrors
- Kalshi or on-chain archive reconstruction (deferred)

## Success demo

```bash
oddsfox init
oddsfox sync markets --active
oddsfox snapshot books --active --top-volume 100
oddsfox compute liquidity --active
oddsfox serve
```
