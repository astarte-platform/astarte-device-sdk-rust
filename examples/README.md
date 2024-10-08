<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Get started using the Rust Astarte Device SDK

The following examples are available to get you started with the Rust Astarte Device SDK:

- [registration](./registration/README.md): shows how to auto register a new device in a local
  instance of astarte using a pairing token.
- [individual datastream](./individual_datastream/README.md): shows how to connect a manually
  registered device to a local instance of Astarte and how to send/receive individual datastreams on
  the device.
- [object datastream](./object_datastream/README.md): shows how to connect a manually registered
  device to a local instance of Astarte and how to send aggregated object datastreams from the
  device.
- [individual properties](./individual_properties/README.md): shows how to connect a manually
  registered device to a local instance of Astarte and how to send/receive property values to/from
  the device. It also shows how to set up data retention using a database.
- [retention](./retention/README.md): this example can be run to test that the data sent to an
  interface with retention `stored` will keep the data, even after loosing power, when disconnected
  from astarte.

# Common prerequisites

All the examples above have some common prerequisites:

- An up-to-date installation of the [rust toolchain](https://www.rust-lang.org/tools/install) for
  your system of choice.
- A local instance of Astarte. See
  [Astarte in 5 minutes](https://docs.astarte-platform.org/astarte/latest/010-astarte_in_5_minutes.html)
  for a quick way to set up Astarte on your machine.
- The [astartectl](https://github.com/astarte-platform/astartectl/releases) tool. We will use
  `astartectl` to manage the Astarte instance.

**N.B.** When installing Astarte using _Astarte in 5 minutes_ perform all the installation steps
until right before the _installing the interfaces_ step.

# Common configuration

Some common configuration is required for all the examples. The only exception is the
[registration](./registration/README.md) example which only requires the generation of a JWT pairing
token.

## Installing the interfaces on Astarte

An interface can be installed by running the following command:

```
astartectl realm-management
    --realm-management-url http://localhost:4000/
    --realm-key <REALM>_private.pem
    --realm-name <REALM>
    interfaces install <INTERFACE_FILE_PATH>
```

Where `<REALM>` is the name of the realm, and `<INTERFACE_FILE_PATH>` is the path name to the
`.json` file containing the interface description. We assume you are running this command from the
Astarte installation folder. If you would like to run it from another location provide the full path
to the realm key.

Each example contains an `/interfaces` folder. To run that example install all the interfaces
contained in the `.json` files in that folder.

## Registering a new device on Astarte (only when manually registering a device)

To manually register the device on the Astarte instance you can use the following `astartectl`
command:

```
astartectl pairing
    --pairing-url http://localhost:4003/
    --realm-key <REALM>_private.pem
    --realm-name <REALM>
    agent register <DEVICE_ID>
```

**NB**: The device id should follow a specific format. See the
[astarte documentation](https://docs.astarte-platform.org/latest/010-design_principles.html#device-id)
for more information regarding accepted values.

**NB**: The credential secret is only shown once during the device registration procedure.

## Generating the Pairing JWT (only when auto registering a device)

We will now generate a Pairing JWT from our Astarte local instance. This JWT will be used by the
device during the initial device registration.

The command to generate a Pairing JWT is:

```
astartectl utils gen-jwt --private-key <REALM_NAME>_private.pem pairing --expiry 0
```

This will generate a never expiring token. To generate a token with an expiration date, then change
`--expiry 0` to `--expiry <SEC>` with `<SEC>` the number of seconds your token should last.

## Configuring the SDK

Each example contains a _configuration.json_ file to be used to configure the example. Each field
can be configured as follows:

- _realm_ - Place here the name of your Astarte realm.
- _device_id_ - Place here the device hardware ID used in the previous step.
- _credentials_secret_ - (only when manually registering a device) Place here the device specific
  credential secret generated in the previous step.
- _pairing_token_ - (only when auto registering a device) Place here the pairing token generated in
  the previous step.
- _pairing_url_ - Place here the Base API URL for your local Astarte instance. (e.g.:
  `http://localhost:4003`).

## Build and run the example

You can start the configured Astarte device with the command:

```
cargo run --example <EXAMPLE_NAME>
```

**N.B.** Run the command above from the root folder of the SDK, not the example folder.

## Device connection to Astarte

Once the example has been started it will automatically attempt to connect the device to the local
instance of Astarte. After the connection has been accomplished, the following log message should be
shown:

```
Connection to Astarte established.
```

You can check the device has been correctly registered and is connected to the Astarte instance
using `astartectl`. To list the all registered devices run:

```
astartectl appengine --appengine-url http://localhost:4002/
    --realm-key <REALM>_private.pem --realm-name <REALM>
    devices list
```

You can check the status of a specific device with the command:

```
astartectl appengine --appengine-url http://localhost:4002/
    --realm-key <REALM>_private.pem --realm-name <REALM>
    devices show <DEVICE_ID>
```
