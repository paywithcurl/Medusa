# Medusa

## About

Medusa is a Pub/Sub system that leverages GenStage.

  You should declare routes in `String` like
  the following examples:

  ```
  Medusa.consume "foo.bar", &Honey.doo/1   # Matches only "foo.bar" events.
  Medusa.consume "foo.*" &Lmbd.bo/1        # Matches all "foo. ..." events
  ```

  Then, to publish something, you call:

  ```
  Medusa.publish "foo.bar", my_awesome_payload
  ```

  ## Caveats

  It can only consume functions of arity 1.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `medusa` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:medusa, "~> 0.1.0"}]
    end
    ```

  2. Ensure `medusa` is started before your application:

    ```elixir
    def application do
      [applications: [:medusa]]
    end
    ```
## Contributing

Run tests with the included test script.
Arguments pasted to this script will be forwarded to mix test.
This allows the test runner to execute single tests.

```
./run_test.sh
./run_test.sh test/my_test:10
```

## Alt

```elixir
defmodule Ching.Publisher do
  use Medusa.Publisher

  @login_requested "user.login"

  def login_request(data = %User.Login{}, ref) do
    metadata = %{id: "id", host: ref.host}
    publish(@login_requested, data, metadata)
  end
end

{:ok, channel} = get_channel
{:ok, message_id} = Ching.Publisher.login_requested(%{user: "steve"}, channel)

defmodule Ching.Consumer do

  consume do
    "foo.*" -> # etc
    "foo.bar" -> # etc
  end
end
```
