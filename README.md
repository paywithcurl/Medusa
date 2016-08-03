# Medusa

## About

Medusa is a Pub/Sub system that leverages GenStage.

  You should declare routes using `Regex` module, like
  the following examples:

  ```
  Medusa.consume ~r/^foo\.bar$/, &Honey.doo/1   # Matches only "foo.bar" events.
  Medusa.consume ~r/^foo\.*/, &Lmbd.bo/1        # Matches all "foo. ..." events.
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

