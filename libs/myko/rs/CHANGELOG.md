# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.1.4 (2025-04-18)

### Chore

 - <csr-id-a37d619a60993b951b836b4306b21a7754fc9773/> actually comment out kafka

### Bug Fixes

 - <csr-id-5ebdd025742e8e0acc54eb7f90f5079e2a905fbe/> actually remove kafka from build deps for rust
 - <csr-id-98ccb04242008c273d4de982c315568edc8d1028/> remove kafka from myko core

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 8 commits contributed to the release over the course of 28 calendar days.
 - 29 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Actually comment out kafka ([`a37d619`](https://github.com/ignition-is-go/rship/commit/a37d619a60993b951b836b4306b21a7754fc9773))
    - Merge remote-tracking branch 'origin/dev' into feat/history ([`c3d898c`](https://github.com/ignition-is-go/rship/commit/c3d898c1aeb6fe58edd5b9ce76b02759e94c4dc4))
    - Merge remote-tracking branch 'origin/dev' into docs ([`837f173`](https://github.com/ignition-is-go/rship/commit/837f173046b2d108d422d2a809a716b794204477))
    - Actually remove kafka from build deps for rust ([`5ebdd02`](https://github.com/ignition-is-go/rship/commit/5ebdd025742e8e0acc54eb7f90f5079e2a905fbe))
    - Merge remote-tracking branch 'origin/dev' into docs ([`38323d9`](https://github.com/ignition-is-go/rship/commit/38323d96dd82bc8262898f82b57446cb52ef7e4b))
    - Merge pull request #270 from ignition-is-go/daily/04-01-2025 ([`24f1f65`](https://github.com/ignition-is-go/rship/commit/24f1f6586cc42e7f610e3b888dffca55c76fba97))
    - Remove kafka from myko core ([`98ccb04`](https://github.com/ignition-is-go/rship/commit/98ccb04242008c273d4de982c315568edc8d1028))
    - Merge branch 'dev' into feat/history ([`cc7a979`](https://github.com/ignition-is-go/rship/commit/cc7a979be48128a12f48bb73626057b242a46f49))
</details>

## v0.1.3 (2025-03-20)

<csr-id-4dd73ff44d9412f090490c8de11411741821261b/>
<csr-id-ee03a43ad332dd8d26f5e47f9744790889ea3d96/>
<csr-id-75d2a73dadbe7bbf57ba6303b406b192c8a4ad3e/>

### Chore

 - <csr-id-4dd73ff44d9412f090490c8de11411741821261b/> clean up rust diagnostics

### New Features

 - <csr-id-bab4b5d02fcf21b6b5badc92d45aa74f771b454e/> files are movin
 - <csr-id-399ad0a1e3b250966670606fd473583df9c4037b/> add reports
 - <csr-id-b07b42c1b38638fd234d978ba3beaee25b53b7fe/> make MykoMessage generic over commands enum

### Bug Fixes

 - <csr-id-c751f05ec833a8be1c13494b00e902134e979180/> disconnect
 - <csr-id-f5b3165ab6ec924a788f5d08211e2867f7f8c58e/> reconnect folders
 - <csr-id-fa57f11fa469b0ca33eaf0865cc7f4c1ab9f7e5b/> big rust rehash
 - <csr-id-aab1aa3c1fa01c64179d75b5fd5ba003542edb87/> query and report watch dont consume client
 - <csr-id-e886eabc12e2fecaabb34c4893cac28e2da98911/> resend query on reconnect and reset state on seq 0
 - <csr-id-8200a096bd0796f2469c9c31b1aa1a3b215e6342/> properly notify disconnect

### Other

 - <csr-id-ee03a43ad332dd8d26f5e47f9744790889ea3d96/> rust query handlers
 - <csr-id-75d2a73dadbe7bbf57ba6303b406b192c8a4ad3e/> rship/myko rust refactor

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 19 commits contributed to the release.
 - 12 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release myko-macros v0.1.2, myko-rs v0.1.3, rship-entities v0.1.3, rship-sdk v0.1.7 ([`821141d`](https://github.com/ignition-is-go/rship/commit/821141dc259547dd14d3c1dba6e06dedc79c550f))
    - Release myko-macros v0.1.2, myko-rs v0.1.3, rship-entities v0.1.3, rship-sdk v0.1.7 ([`d62808b`](https://github.com/ignition-is-go/rship/commit/d62808b17b21beac2754f9e3327e4503626db981))
    - Merge pull request #202 from ignition-is-go/feat/remote-routes ([`eaecbb3`](https://github.com/ignition-is-go/rship/commit/eaecbb3c741baf7898db155da727204c3284e3c3))
    - Clean up rust diagnostics ([`4dd73ff`](https://github.com/ignition-is-go/rship/commit/4dd73ff44d9412f090490c8de11411741821261b))
    - Merge pull request #173 from ignition-is-go/fix/ui ([`2ab7d5c`](https://github.com/ignition-is-go/rship/commit/2ab7d5c2b3f517d8b8d2fbc2d05f6df7b50869cc))
    - Rust query handlers ([`ee03a43`](https://github.com/ignition-is-go/rship/commit/ee03a43ad332dd8d26f5e47f9744790889ea3d96))
    - Rship/myko rust refactor ([`75d2a73`](https://github.com/ignition-is-go/rship/commit/75d2a73dadbe7bbf57ba6303b406b192c8a4ad3e))
    - Merge pull request #171 from ignition-is-go/feat/asset-movement ([`2be13e4`](https://github.com/ignition-is-go/rship/commit/2be13e4bbe3604a92026982d8adb6c4645fd8fa8))
    - Disconnect ([`c751f05`](https://github.com/ignition-is-go/rship/commit/c751f05ec833a8be1c13494b00e902134e979180))
    - Reconnect folders ([`f5b3165`](https://github.com/ignition-is-go/rship/commit/f5b3165ab6ec924a788f5d08211e2867f7f8c58e))
    - Big rust rehash ([`fa57f11`](https://github.com/ignition-is-go/rship/commit/fa57f11fa469b0ca33eaf0865cc7f4c1ab9f7e5b))
    - Files are movin ([`bab4b5d`](https://github.com/ignition-is-go/rship/commit/bab4b5d02fcf21b6b5badc92d45aa74f771b454e))
    - Query and report watch dont consume client ([`aab1aa3`](https://github.com/ignition-is-go/rship/commit/aab1aa3c1fa01c64179d75b5fd5ba003542edb87))
    - Merge pull request #170 from ignition-is-go/feat/torrent ([`2004fbf`](https://github.com/ignition-is-go/rship/commit/2004fbff033491378b979c975daa16bf1315488c))
    - Add reports ([`399ad0a`](https://github.com/ignition-is-go/rship/commit/399ad0a1e3b250966670606fd473583df9c4037b))
    - Make MykoMessage generic over commands enum ([`b07b42c`](https://github.com/ignition-is-go/rship/commit/b07b42c1b38638fd234d978ba3beaee25b53b7fe))
    - Resend query on reconnect and reset state on seq 0 ([`e886eab`](https://github.com/ignition-is-go/rship/commit/e886eabc12e2fecaabb34c4893cac28e2da98911))
    - Properly notify disconnect ([`8200a09`](https://github.com/ignition-is-go/rship/commit/8200a096bd0796f2469c9c31b1aa1a3b215e6342))
    - Merge pull request #167 from ignition-is-go/feat/files ([`38648b5`](https://github.com/ignition-is-go/rship/commit/38648b5025aa9b00aa578d1d9ecfcd624c5efb71))
</details>

## v0.1.2 (2024-12-03)

### Bug Fixes

 - <csr-id-6ddc40322d4af46c75c4b690ebf9afbfdf5a33e2/> wait till channels are open to return connected

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 day passed between releases.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release myko-rs v0.1.2, rship-entities v0.1.2, rship-sdk v0.1.5 ([`69f3a46`](https://github.com/ignition-is-go/rship/commit/69f3a460f03b4d340395956d4a41a32ae8370498))
    - Wait till channels are open to return connected ([`6ddc403`](https://github.com/ignition-is-go/rship/commit/6ddc40322d4af46c75c4b690ebf9afbfdf5a33e2))
</details>

## v0.1.1 (2024-12-02)

<csr-id-30a94114c1f86c7527a958281f79c5bed6360a2a/>

### Chore

 - <csr-id-30a94114c1f86c7527a958281f79c5bed6360a2a/> bump versions

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release myko-macros v0.1.1, myko-wasm v0.1.1, myko-rs v0.1.1, rship-entities v0.1.1, rship-sdk v0.1.1 ([`91ab979`](https://github.com/ignition-is-go/rship/commit/91ab979a04bf5558d1cebec1e1ff571c31f0b8c0))
    - Bump versions ([`30a9411`](https://github.com/ignition-is-go/rship/commit/30a94114c1f86c7527a958281f79c5bed6360a2a))
</details>

## v0.1.0 (2024-12-02)

<csr-id-c4f8e449ea5c9b16b8477e99a1468042db770037/>
<csr-id-5beca017fbc44878128e52a033e51b755b45a138/>
<csr-id-c455d174207d7c8a29cd021f8e739db5fce673c1/>
<csr-id-c8af9d9cb226fe181350cb42ef7d841eebfc50d9/>
<csr-id-baf2c8e2d86faeadd46aedaf170326b0e1720a19/>
<csr-id-58fd22f88c42cf4d6d43604468e91f41ae4eb803/>
<csr-id-980407db6cd55835c205e20a9c068331597d2c80/>
<csr-id-160408961d3d08bab0d2d2503f313cdc15ba190a/>
<csr-id-9701633c121317a9ac465f0c4341e31cab5c4856/>
<csr-id-b1a02e395e87af64be82db68c9e2ccccb5666452/>
<csr-id-6c4f366b17903baddc95c0f3c2690a10fae56b2b/>
<csr-id-e16ba190c7365a8fe966c63e4d5bd144cd279303/>
<csr-id-ee2d4434569e94b59d587afb1e8987289fa7ee6a/>
<csr-id-edd280215235cdc44cf34d66ec401f235c837e1b/>
<csr-id-e96574d0f74e40d21c80e901bcb56ae45f2b1452/>
<csr-id-f0887e066851473dc5e455804e299772e0f4a17b/>
<csr-id-07856acd96c826183fcabf9771259b6a5c7c18e6/>
<csr-id-ce4eccabd74fdadf6a04633a04518c370ff3f413/>

### Chore

 - <csr-id-c4f8e449ea5c9b16b8477e99a1468042db770037/> update version and references
 - <csr-id-5beca017fbc44878128e52a033e51b755b45a138/> rd-kafka cmake flags for windows
 - <csr-id-c455d174207d7c8a29cd021f8e739db5fce673c1/> make and use autoreconnect websocket
 - <csr-id-c8af9d9cb226fe181350cb42ef7d841eebfc50d9/> clean up logs
 - <csr-id-baf2c8e2d86faeadd46aedaf170326b0e1720a19/> moar clippy!
 - <csr-id-58fd22f88c42cf4d6d43604468e91f41ae4eb803/> cleanup
 - <csr-id-980407db6cd55835c205e20a9c068331597d2c80/> reorg
 - <csr-id-160408961d3d08bab0d2d2503f313cdc15ba190a/> reorg

### Chore

 - <csr-id-ce4eccabd74fdadf6a04633a04518c370ff3f413/> update add descriptions and licenses

### Chore

 - <csr-id-07856acd96c826183fcabf9771259b6a5c7c18e6/> add changelogs

### New Features

 - <csr-id-27598fcb49fe5a50df9662dae616f8391883bcad/> update rust autoreconnect socket
 - <csr-id-d989a4a47887937042dea7c2531751217eae7b00/> myko client and sdk no longer need clientId
 - <csr-id-331a012874397ec53b15affdd29cf4a1841026e8/> auto resend instances on new client id
 - <csr-id-7f37dbbfdbabbd8ccd7bed133759a4b0507da1b0/> support watch client_id
 - <csr-id-1742f8e4f53234eb13a8a604300507f3998243af/> much sdk
 - <csr-id-d22a93423fba64fe7fb7f1d78b6e7aea23f82b3f/> add broacast fallback
 - <csr-id-c5619de1d04a4fe1661b92d9be9f22cae32b9fc0/> add kafka
 - <csr-id-8486298d5be0802c42233e7c8aeb4b512b4dd9e0/> queries work
 - <csr-id-65e29b1e21f88ec55d7ed580d81c0ab9377c8fe9/> add watch
 - <csr-id-eda10c99822ad0f418165aa2c101c09c28665b89/> auto build module
 - <csr-id-89f711615ba4c4771c109f3daa833e26e6a4370b/> add proc macros
 - <csr-id-a748b1858d70bb80de9c0cf7fdc8dd591ac1daa3/> rust repos with query!
 - <csr-id-34c86519d88a7ab0813cc31c8a93622c84215c7d/> even more queries
 - <csr-id-bd7f8df3acb25df5240131c88de41b2e05cf6623/> watchId query
 - <csr-id-23a16c722e68427f883d12427202a69a65aaba12/> include backplane in event bus

### Bug Fixes

 - <csr-id-e10d11701c3e7f957ebd21750162ca19bada2f82/> update client id commands
 - <csr-id-84fa321a6215ba620ee59a18c3f8de0ddfa8837c/> auto connect to last sucessful server
 - <csr-id-1fb28cd325b310c5401b5250bc59993fd4fe3e40/> imports and build
 - <csr-id-091c827b7618dbd3b01e1016d50818cbdbdd8838/> clean up teardown
 - <csr-id-95453b4b4e28d652391e488ac393862c42c3ecb0/> client is now clone
 - <csr-id-e1a00b6c8c24f555736c73b627062fb3f5bcaed0/> clean up rust
 - <csr-id-eb50eabc4777db79b06c901d903fd2773882ee59/> public module and no expect

### Other

 - <csr-id-9701633c121317a9ac465f0c4341e31cab5c4856/> file browser
 - <csr-id-b1a02e395e87af64be82db68c9e2ccccb5666452/> begin add files to machines
 - <csr-id-6c4f366b17903baddc95c0f3c2690a10fae56b2b/> rust sdk and button emitter
 - <csr-id-e16ba190c7365a8fe966c63e4d5bd144cd279303/> rust
 - <csr-id-ee2d4434569e94b59d587afb1e8987289fa7ee6a/> getting there
 - <csr-id-edd280215235cdc44cf34d66ec401f235c837e1b/> myko event refactor
 - <csr-id-e96574d0f74e40d21c80e901bcb56ae45f2b1452/> rust repo
 - <csr-id-f0887e066851473dc5e455804e299772e0f4a17b/> queries

### New Features (BREAKING)

 - <csr-id-1f61b3ec95d4e3d59d682e196416a0e1e98539b5/> add rust queries, move txids inside data

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 52 commits contributed to the release over the course of 380 calendar days.
 - 41 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Bump myko-macros v0.1.0, myko-wasm v0.1.0, myko-rs v0.1.0, rship-entities v0.1.0, rship-sdk v0.1.0 ([`be34bbc`](https://github.com/ignition-is-go/rship/commit/be34bbca0ff802c6e40061f565b772488e7ed00d))
    - Update add descriptions and licenses ([`ce4ecca`](https://github.com/ignition-is-go/rship/commit/ce4eccabd74fdadf6a04633a04518c370ff3f413))
    - Release myko-macros v0.1.0, myko-wasm v0.1.0, myko-rs v0.1.0, rship-entities v0.1.0, rship-sdk v0.1.0 ([`5edc6dc`](https://github.com/ignition-is-go/rship/commit/5edc6dc0b154e5019ec38dee85f6fbc7bb1389a4))
    - Add changelogs ([`07856ac`](https://github.com/ignition-is-go/rship/commit/07856acd96c826183fcabf9771259b6a5c7c18e6))
    - Update version and references ([`c4f8e44`](https://github.com/ignition-is-go/rship/commit/c4f8e449ea5c9b16b8477e99a1468042db770037))
    - File browser ([`9701633`](https://github.com/ignition-is-go/rship/commit/9701633c121317a9ac465f0c4341e31cab5c4856))
    - Add rust queries, move txids inside data ([`1f61b3e`](https://github.com/ignition-is-go/rship/commit/1f61b3ec95d4e3d59d682e196416a0e1e98539b5))
    - Begin add files to machines ([`b1a02e3`](https://github.com/ignition-is-go/rship/commit/b1a02e395e87af64be82db68c9e2ccccb5666452))
    - Update rust autoreconnect socket ([`27598fc`](https://github.com/ignition-is-go/rship/commit/27598fcb49fe5a50df9662dae616f8391883bcad))
    - Myko client and sdk no longer need clientId ([`d989a4a`](https://github.com/ignition-is-go/rship/commit/d989a4a47887937042dea7c2531751217eae7b00))
    - Merge pull request #128 from ignition-is-go/exec/music-analysis ([`35a14d8`](https://github.com/ignition-is-go/rship/commit/35a14d81e9a5ef7bb387cf3db99a447f3b36613d))
    - Update client id commands ([`e10d117`](https://github.com/ignition-is-go/rship/commit/e10d11701c3e7f957ebd21750162ca19bada2f82))
    - Merge pull request #59 from ignition-is-go/release/canary ([`637b9fa`](https://github.com/ignition-is-go/rship/commit/637b9fae2a2f68c8bdf80d5be5474a18de0354ea))
    - Merge pull request #57 from ignition-is-go/dev ([`3bbab82`](https://github.com/ignition-is-go/rship/commit/3bbab827309e2e76c791412571dc55eec140c288))
    - Merge pull request #55 from ignition-is-go/feat/ent-lib ([`90e6851`](https://github.com/ignition-is-go/rship/commit/90e6851902e151b7d0e890aa3c3248c4b98cb506))
    - Fix: rust build ([`a19bf20`](https://github.com/ignition-is-go/rship/commit/a19bf208217afa213a56eecac7121a0ad9d10e19))
    - Merge pull request #1352 from ignition-is-go/release/canary ([`16619ad`](https://github.com/ignition-is-go/rship/commit/16619adf23cda1f1222401d8dd125c82212c7c5a))
    - Merge pull request #1355 from ignition-is-go/feat/installer ([`cad31a1`](https://github.com/ignition-is-go/rship/commit/cad31a1f3f7f97f23e2249e6b5574d84f47daba2))
    - Auto connect to last sucessful server ([`84fa321`](https://github.com/ignition-is-go/rship/commit/84fa321a6215ba620ee59a18c3f8de0ddfa8837c))
    - Rd-kafka cmake flags for windows ([`5beca01`](https://github.com/ignition-is-go/rship/commit/5beca017fbc44878128e52a033e51b755b45a138))
    - Merge pull request #1345 from ignition-is-go/feat/controllers ([`57c5fa8`](https://github.com/ignition-is-go/rship/commit/57c5fa8b7c22e041a3e20b92478f49b69a38bd88))
    - Imports and build ([`1fb28cd`](https://github.com/ignition-is-go/rship/commit/1fb28cd325b310c5401b5250bc59993fd4fe3e40))
    - Auto resend instances on new client id ([`331a012`](https://github.com/ignition-is-go/rship/commit/331a012874397ec53b15affdd29cf4a1841026e8))
    - Support watch client_id ([`7f37dbb`](https://github.com/ignition-is-go/rship/commit/7f37dbbfdbabbd8ccd7bed133759a4b0507da1b0))
    - Make and use autoreconnect websocket ([`c455d17`](https://github.com/ignition-is-go/rship/commit/c455d174207d7c8a29cd021f8e739db5fce673c1))
    - Much sdk ([`1742f8e`](https://github.com/ignition-is-go/rship/commit/1742f8e4f53234eb13a8a604300507f3998243af))
    - Clean up logs ([`c8af9d9`](https://github.com/ignition-is-go/rship/commit/c8af9d9cb226fe181350cb42ef7d841eebfc50d9))
    - Clean up teardown ([`091c827`](https://github.com/ignition-is-go/rship/commit/091c827b7618dbd3b01e1016d50818cbdbdd8838))
    - Moar clippy! ([`baf2c8e`](https://github.com/ignition-is-go/rship/commit/baf2c8e2d86faeadd46aedaf170326b0e1720a19))
    - Client is now clone ([`95453b4`](https://github.com/ignition-is-go/rship/commit/95453b4b4e28d652391e488ac393862c42c3ecb0))
    - Rust sdk and button emitter ([`6c4f366`](https://github.com/ignition-is-go/rship/commit/6c4f366b17903baddc95c0f3c2690a10fae56b2b))
    - Rust ([`e16ba19`](https://github.com/ignition-is-go/rship/commit/e16ba190c7365a8fe966c63e4d5bd144cd279303))
    - Merge pull request #1344 from ignition-is-go/feat/link ([`31c12c6`](https://github.com/ignition-is-go/rship/commit/31c12c615fd15fe44e35ef032f889859896a93d7))
    - Clean up rust ([`e1a00b6`](https://github.com/ignition-is-go/rship/commit/e1a00b6c8c24f555736c73b627062fb3f5bcaed0))
    - Cleanup ([`58fd22f`](https://github.com/ignition-is-go/rship/commit/58fd22f88c42cf4d6d43604468e91f41ae4eb803))
    - Add broacast fallback ([`d22a934`](https://github.com/ignition-is-go/rship/commit/d22a93423fba64fe7fb7f1d78b6e7aea23f82b3f))
    - Add kafka ([`c5619de`](https://github.com/ignition-is-go/rship/commit/c5619de1d04a4fe1661b92d9be9f22cae32b9fc0))
    - Public module and no expect ([`eb50eab`](https://github.com/ignition-is-go/rship/commit/eb50eabc4777db79b06c901d903fd2773882ee59))
    - Queries work ([`8486298`](https://github.com/ignition-is-go/rship/commit/8486298d5be0802c42233e7c8aeb4b512b4dd9e0))
    - Getting there ([`ee2d443`](https://github.com/ignition-is-go/rship/commit/ee2d4434569e94b59d587afb1e8987289fa7ee6a))
    - Myko event refactor ([`edd2802`](https://github.com/ignition-is-go/rship/commit/edd280215235cdc44cf34d66ec401f235c837e1b))
    - Add watch ([`65e29b1`](https://github.com/ignition-is-go/rship/commit/65e29b1e21f88ec55d7ed580d81c0ab9377c8fe9))
    - Auto build module ([`eda10c9`](https://github.com/ignition-is-go/rship/commit/eda10c99822ad0f418165aa2c101c09c28665b89))
    - Reorg ([`980407d`](https://github.com/ignition-is-go/rship/commit/980407db6cd55835c205e20a9c068331597d2c80))
    - Add proc macros ([`89f7116`](https://github.com/ignition-is-go/rship/commit/89f711615ba4c4771c109f3daa833e26e6a4370b))
    - Reorg ([`1604089`](https://github.com/ignition-is-go/rship/commit/160408961d3d08bab0d2d2503f313cdc15ba190a))
    - Rust repos with query! ([`a748b18`](https://github.com/ignition-is-go/rship/commit/a748b1858d70bb80de9c0cf7fdc8dd591ac1daa3))
    - Rust repo ([`e96574d`](https://github.com/ignition-is-go/rship/commit/e96574d0f74e40d21c80e901bcb56ae45f2b1452))
    - Even more queries ([`34c8651`](https://github.com/ignition-is-go/rship/commit/34c86519d88a7ab0813cc31c8a93622c84215c7d))
    - WatchId query ([`bd7f8df`](https://github.com/ignition-is-go/rship/commit/bd7f8df3acb25df5240131c88de41b2e05cf6623))
    - Queries ([`f0887e0`](https://github.com/ignition-is-go/rship/commit/f0887e066851473dc5e455804e299772e0f4a17b))
    - Include backplane in event bus ([`23a16c7`](https://github.com/ignition-is-go/rship/commit/23a16c722e68427f883d12427202a69a65aaba12))
</details>

