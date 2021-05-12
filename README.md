# Rocketship

## Client

This is an angular application which serves as the primary client interface for Rocketship

## Server

This is a NodeJS application which is the core backend process for Rocketship

## Sherlock

An asset management backend

## Intake

A sidecar application that handles the creation of schedule data via a REST endpoint

## Discovery

A sidecar application that handles forwarding discovery packets into the docker network from outside. 



# Developer Style Guide

- Notes
- Best Practices

## Dev Notes

- TODO & NOTE comments should include author's initials

```ts
// TODO(ts): need to implement
// NOTE(ts): informational message
```
### Naming

- for mutation which makes `upsert` operation should start with `save`, for example: `saveCard`, `saveCustomer`
- for mutation where we need to have different logic for create and update, we should name them as `createUser`, `updateUser`
- we should try to have `save` mutations by default and `create/update` for special cases

### Props

- props for `save` mutations should look like this: `saveCard(id: ID, data: CardInput!)`, we should declare inputs for data, id isn't required

### Types

- internal types should be declared after mutation function

## Best Practices

- Try to use db in mutations, if there will be loader used, please make a comment why
- Try to use loader in queries, when you are querying by id(s), if there will be necessary to use db and query by id, please make a note also
- Delete mutations and service methods should return a boolean value.


### Graph Client - Local Cache

GraphQL is used for local cache store. Default way for updating cache is refetch query, you can use generated refetchQuery from `graph.service.ts`

---

This workspace was generated using [Nx](https://nx.dev).

## Development server

Run `nx serve my-app` for a dev server. The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng g component my-component --project=my-app` to generate a new component.

## Build

Run `nx build my-app` to build the project. The build artifacts will be stored in the `dist/` directory. Use the `--prod` flag for a production build.

## Running unit tests

Run `nx test my-app` to execute the unit tests via [Jest](https://jestjs.io).

Run `nx affected:test` to execute the unit tests affected by a change.

## Running end-to-end tests

Run `nx e2e my-app` to execute the end-to-end tests via [Cypress](https://www.cypress.io).

Run `nx affected:e2e` to execute the end-to-end tests affected by a change.

## Understand your workspace

Run `nx dep-graph` to see a diagram of the dependencies of your projects.
