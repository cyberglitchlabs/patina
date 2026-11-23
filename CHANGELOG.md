# Changelog

## [0.1.2](https://github.com/cyberglitchlabs/patina/compare/v0.1.1...v0.1.2) (2026-11-23)


### Features

* **chart:** chart skeleton, values contract and schema ([204c5d8](https://github.com/cyberglitchlabs/patina/commit/204c5d81cd0251cf18ab7c3596f8b2e2fc8c46a2))
* **chart:** config ConfigMap with TOML validation ([eb22681](https://github.com/cyberglitchlabs/patina/commit/eb22681a4f61c170fe7d3ae3bada190ca95a260d))
* **chart:** deployment, volume and service ([189911a](https://github.com/cyberglitchlabs/patina/commit/189911a6601c43d6b5f621c706a6923780933618))
* **chart:** generated tokens kept across upgrades ([d32f3b8](https://github.com/cyberglitchlabs/patina/commit/d32f3b883b609389d86d98550364b4df71f3073c))
* **chart:** Helm chart with lint, test and scan automation ([a9e2ced](https://github.com/cyberglitchlabs/patina/commit/a9e2ced5f796c7e6dcd8502efc7bee2611f58d39))
* **chart:** optional ingress, servicemonitor and networkpolicy ([17d6ca0](https://github.com/cyberglitchlabs/patina/commit/17d6ca00644478b9a153850bcef4b323ce1f8d80))


### Bug Fixes

* **chart:** final review fixes (tokens in CI, real checksum test, default resources in values, label and interval handling, docs) ([6aad169](https://github.com/cyberglitchlabs/patina/commit/6aad1696ba1a064c20a4ca2dd270612d9d82287b))
* **chart:** require exact overrides path and tighten Error check ([6f33931](https://github.com/cyberglitchlabs/patina/commit/6f339319faf50cf74d51fa938a73d93c59b70a47))

## [0.1.1](https://github.com/cyberglitchlabs/patina/compare/v0.1.0...v0.1.1) (2026-11-22)


### Bug Fixes

* floor negative sub-second times; tidy leftovers from the jiff move ([6c6be16](https://github.com/cyberglitchlabs/patina/commit/6c6be1631902b392bf5b81c5f2b6ff7d88e6df3d))
* **gracenote:** serve programme artwork from zpmc.tmsimg.com ([ad83b76](https://github.com/cyberglitchlabs/patina/commit/ad83b76db93936a792d180823fc6af11e4548ebd))
* **ingest:** keep stored hindsight averages when they cannot be loaded ([57b6953](https://github.com/cyberglitchlabs/patina/commit/57b6953dcd88d4ff3b860133d6a993e185e4629f))
* **ingest:** keep stored hindsight averages when they cannot be loaded ([5fccff9](https://github.com/cyberglitchlabs/patina/commit/5fccff94471bd3458513cda87348cefcc3e1490a))
* **store:** leave a quoted ? alone when numbering placeholders; refresh docs ([2c45941](https://github.com/cyberglitchlabs/patina/commit/2c45941bd7b4f1abbc875f84826db0347c789416))
* **xmltv:** add a bare channel number display-name so Jellyfin can match channels ([95259cb](https://github.com/cyberglitchlabs/patina/commit/95259cbee20366e04ce18b8d386b4976643df146))
* **xmltv:** skip times with second-level offsets; require exact zone names ([7e39087](https://github.com/cyberglitchlabs/patina/commit/7e390870a017e5883c45e1d57f355b07978c0e42))
