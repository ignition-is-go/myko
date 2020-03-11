# Rocketship

## Client

This is an angular application which serves as the primary client interface for Rocketship

## Server

This is a NodeJS application which is the core backend process for Rocketship

## Satellite

Satellite is an electron + angular project that connects a separate computer with the Rocketship server

### Dev Notes

Any main thread dependencies for satellite need to exist both in the dev dependencies of the top level `./package.json`, as well as the dependencies of the `apps/satellite/package.json` in order to both debug and package properly.

# Storybook

In order to use storybook with components that consume GraphQL, you can use the `apolloStorybookDecorator` to mock the GraphQL server.

```typescript
// ...other imports...
import apolloStorybookDecorator from 'libs/apollo-storybook-angular'

storiesOf('My Component', module).addDecorator(
  apolloStorybookDecorator({
    typeDefs: schema,
    mocks: {},
    typeResolvers: {},
  }),
)
```

More usage example is here

You can import the server schema via:

```typescript
import { schema } from 'generated/server.schema'
```

# Local Cache

GraphQL is used for local cache store. Default way for updating cache is refetch query, you can use generated refetchQuery from `graph.service.ts`

## Docker container

If for whatever reason you need to build and push the docker image of the client locally, run the following (replace dev with whatever channel you need to build for):

```bash
CONFIG=dev
npm run ng -- build rship-client --configuration ${CONFIG} && (docker build -t registry.rship.io/rocketship-client:${CONFIG} .) && docker push registry.rship.io/rocketship-client:${CONFIG}
```

---

# Developer Style Guide

- Notes
- Best Parctices

## Dev Notes

- TODO & NOTE comments should include author's initials

```ts
// TODO(ez): need to implement
// NOTE(ez): informational message
```

## Mutations

Each mutation should have an individual file

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

---

This project was generated using [Nx](https://nx.dev).

🔎 **Nx is a set of Angular CLI power-ups for modern development.**

## Development server

Run `ng serve my-app` for a dev server. Navigate to http://localhost:4200/. The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng g component my-component --project=my-app` to generate a new component.

## Build

Run `ng build my-app` to build the project. The build artifacts will be stored in the `dist/` directory. Use the `--prod` flag for a production build.

## Running unit tests

Run `ng test my-app` to execute the unit tests via [Jest](https://jestjs.io).

Run `npm run affected:test` to execute the unit tests affected by a change.

## Running end-to-end tests

Run `ng e2e my-app` to execute the end-to-end tests via [Cypress](https://www.cypress.io).

Run `npm run affected:e2e` to execute the end-to-end tests affected by a change.

## Understand your workspace

Run `npm run dep-graph` to see a diagram of the dependencies of your projects.
