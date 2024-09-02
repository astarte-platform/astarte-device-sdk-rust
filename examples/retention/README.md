<!--
Copyright 2024 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Retention Example

This example demonstrates how data retention works in Astarte. Specifically, it tests that data sent
to an interface with retention set to `stored` will be preserved even after a power loss, when
disconnected from Astarte.

## Overview

The retention setting is critical for scenarios where data loss cannot be tolerated, even in the
face of temporary disconnections or power failures. By setting the retention to `stored`, the data
is ensured to be stored persistently until it can be successfully transmitted to Astarte.

## Running the Example

Follow these steps to run the retention example:

1. **Keep Astarte Unreachable:** Ensure that Astarte is disconnected and unreachable from the
   device.

1. **Run the Example:**
   - Navigate to the `retention` directory.
   - Deploy and run the example code on your device by executing:
     ```
     cargo run --example retention
     ```
   - The example will start sending data while Astarte is disconnected.

1. **Simulate Power Loss:**
   - Stop the example to simulate a power loss on the device.
   - Restart the example on the device using the same command:
     ```
     cargo run --example retention
     ```

1. **Make Astarte Reachable:**
   - Reconnect Astarte and make it reachable from the device.

## Verification

After the device reconnects to Astarte:

- Access the Astarte interface’s data.
- Check to ensure that the data sent prior to the simulated power loss and disconnection is present.

To check if there are properties in the SQLite store, you can run:

```sh
echo 'SELECT * FROM retention_publish;' | sqlite3 /tmp/astarte-example-retention/prop-cache.db
```

This confirms the retention functionality is working as expected.

## Conclusion

This example highlights the robustness of Astarte's data handling capabilities, ensuring critical
data is not lost due to unforeseen power interruptions.

For additional information, refer to the official
[Astarte documentation](https://docs.astarte-platform.org).
