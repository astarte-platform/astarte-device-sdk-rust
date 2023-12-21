<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Properties example

This example will guide you through setting up a simple application running the Astarte SDK and
interacting with a local instance of Astarte.

The device generated with this example will perform the following:
- Use permanent storage for storing all the SDK instance data.
- Transmit periodically a change of the name property for a single virtual sensor.
- Receive and store two properties, an enable property and a sampling period property for each
virtual sensor.

If you have not already done so, make sure that you satisfy the
[common prerequisites](./../README.md#common-prerequisites) and have completed the
[common configuration](./../README.md#common-configuration) for the examples.

## Observe device properties transition

Upon connecting to the local Astarte instance, the instance SDK will start updating periodically
the value of one of is properties.

We can check if the publish from the device has been correctly received on the server using
`data-snapshot` as follows:
```
astartectl appengine
    --appengine-url http://localhost:4002/ --realm-management-url http://localhost:4000/
    --realm-key <REALM_NAME>_private.pem --realm-name <REALM_NAME>
    devices data-snapshot <DEVICE_NAME>
```
Where `<REALM_NAME>` is your realm's and `<DEVICE_NAME>` is the device ID from which the data
has been received.

This will show a summary of all properties set for this device. Repeat this command between
changes of the "name" properties in the SDK and observe how the changes are reflected in the
remote instance of Astarte.

## Observe server properties transitions

We can use the `astartectl` tool to change the value of a server owned property.
Two server owned properties are present in this example, an `enable` and a `samplingPeriod`
property.

With `send-data` we can publish new values on a server-owned interface.
The syntax is the following:
```
astartectl appengine
    --appengine-url http://localhost:4002/ --realm-management-url http://localhost:4000/
    --realm-key <REALM>_private.pem
    --realm-name <REALM> devices send-data <DEVICE_ID>
    org.astarte-platform.rust.examples.individual-properties.ServerProperties <ENDPOINT> <VALUE>
```
Where `<REALM>` is your realm name, `<DEVICE_ID>` is the device ID to send the data to,
`<ENDPOINT_TYPE>` is the Astarte type of the chosen endpoint, `<ENDPOINT>` is the endpoint
to send data to, which in this example should be composed by a sensor id and one of the two endpoints
(e.g. `/42/enable`), and `<VALUE>` is the value to send.

On you device console you should see a message confirming the correct sensor has been enabled or
disabled.

## Observe device properties retention

Let's now observe how properties values are retained between different instances of the SDK.
Remember that the party responsible to retain the properties is the one with ownership over the
interface containing the property.

For server owned interfaces, our remote instance of Astarte will take care of keeping track
of the property value. We can see that by enabling or disabling a sensor as described above and
then terminating the device execution.
At its next startup the device will query the value of the properties to the server and will print
to screen the enable/disable value of the sensors you updated before.

For device owned interfaces the properties values are stored in a local database.
We can observe the "name" property for sensor 1 and we will notice how each 5 seconds this property
value is updated using an incremental counter.
We can terminate the device execution and then start a new instance.
We will notice how the last value of our property is printed to screen and the "name" property
update restarts from the last counter used before termination.
