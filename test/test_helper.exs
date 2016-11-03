ExUnit.configure(exclude: [rabbitmq: true, pg2: true])  # FIXME PG2 test setup has problem with cluster

ExUnit.start
