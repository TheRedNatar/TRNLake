defmodule TLake.MixProject do
  use Mix.Project

  def project do
    [
      app: :t_lake,
      version: "1.0.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases(),
      aliases: aliases(),
      dialyzer: [
        flags: [:unmatched_returns, :error_handling, :underspecs, :extra_return, :missing_return]
      ],
      preferred_cli_env: [
        "test.reset": :test,
        "test.integration": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {TLake.Application, []}
    ]
  end

  defp aliases do
    [
      lint: [
        "format",
        "dialyzer"
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # No runtime deps
      {:dialyxir, ">= 1.4.3", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.32.2", only: :dev, runtime: false},
      {:propcheck, "~> 1.4", only: [:test, :dev]},
      {:rustler, ">= 0.0.0", runtime: false},

      # Runtime deps
      {:jason, "~> 1.4"},
      {:travianmap, "1.1.0"},
      {:gen_stage, "~> 1.2"},
      {:nx, "0.7.3"},
      # {:exgboost, "0.5.0"},
      {:explorer, "0.9.0", system_env: %{"EXPLORER_BUILD" => "1"}}
    ]
  end

  defp releases() do
    [
      t_lake: [
        # config_providers: [
        #   {Config.Reader, {:system, "RELEASE_ROOT", "/shadow_config.exs"}}
        # ],
        include_executables_for: [:unix]
      ]
    ]
  end
end
