ExUnit.configure(exclude: [clustered: true])

exclude = Keyword.get(ExUnit.configuration(), :exclude, [])

unless :clustered in exclude, do: Medusa.Cluster.spawn

ExUnit.start
