ExUnit.configure(exclude: [rabbitmq: true, pg2: true, skip: true])  # FIXME PG2 test setup has problem with cluster

ExUnit.start
