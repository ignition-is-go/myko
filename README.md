# Rocketship

<div style="text-align: center;">
  <img src="assets/icons/rocketship.png" alt="Logo" width="100" height="100">
  <br>
</div>

#### Status 


[![Asset](https://github.com/ignition-is-go/rship/actions/workflows/pub-asset.yml/badge.svg)](https://github.com/ignition-is-go/rship/actions/workflows/pub-asset.yml) 
[![Docker](https://github.com/ignition-is-go/rship/actions/workflows/pub-docker.yml/badge.svg)](https://github.com/ignition-is-go/rship/actions/workflows/pub-asset.yml) 
[![Link](https://github.com/ignition-is-go/rship/actions/workflows/pub-win-mac.yml/badge.svg)](https://github.com/ignition-is-go/rship/actions/workflows/pub-asset.yml)
[![JSR](https://github.com/ignition-is-go/rship/actions/workflows/pub-jsr.yml/badge.svg)](https://github.com/ignition-is-go/rship/actions/workflows/pub-asset.yml)


## Realtime Reactive Event Relationships

[Rocketship](https://docs.rship.io) **(rship) is a centralized control platform for orchestrating reactive event relationships within networks of integrated multimedia systems.** It provides an abstract, system-agnostic language and interface which makes it straightforward and intuitive to design and visualize immersive experiences, so that users can focus on realizing their ideas rather than having to build bespoke solutions for every project. By lowering the technical burden for setting up interactive environments, rship facilitates a more flexible and powerful creative design process.

## Core Entities

A physical **Machine** running a local **Instance** of some software **Service** connects to an rship server over websocket via an **Executor** client. The Executor publishes **Targets** to rship that represent interactable entities within the Service. 

Targets have **Emitters** and/or **Actions**: Emitters observe the Target and send data **Pulses** to rship when its state changes, and Actions are commands made by rship carrying optional data **Payloads** that change the state of the Target.

**Bindings** represent realtime reactive event relationships between Emitters and Actions, and are encapsulated within **Scenes**.

Local timing within a Scene is controlled via **Event Tracks**, which can be created manually or sourced from Executors.

Global timing is controlled by placing Scenes onto **Calendars**, which schedules their activation and deactivation.

See the [user guide](https://docs.rship.io) for a complete reference.

## Executor Downloads

Official releases of rship Executors can be found [on the rship website](https://docs.rship.io/releases) as well as [on github](https://github.com/ignition-is-go/rship-release/releases).

## Contributing

We welcome contributions from the community via pull requests.

If you would like to integrate a system with rship by writing an Executor, please consult the [Executor API](https://docs.rship.io/executor-development/api).

### Commits

Commit messages drive our release notes and CI workflows.

Commits which add a feature should include  
`feat(scope): and a message describing the functionality`

Commits which fix a bug should include  
`fix(scope): and a message describing the fix`

Please refer to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0-beta.4/) for the complete docs on how to commit to this repo.

### Developer Style Guide

- TODO & NOTE comments should include the author's initials
```ts
// TODO(ts): need to implement
// NOTE(ts): informational message
```

## License
