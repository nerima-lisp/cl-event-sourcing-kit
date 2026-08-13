{
  description = "Domain-independent event sourcing protocol for Common Lisp";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    cl-nix-forge = {
      url = "github:nerima-lisp/cl-nix-forge/v0.5.0";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    cl-weave = {
      url = "github:nerima-lisp/cl-weave/v1.3.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.paredit-cli.follows = "paredit-cli";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    paredit-cli = {
      url = "github:nerima-lisp/paredit-cli/v1.6.0";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    cl-prolog = {
      url = "github:nerima-lisp/cl-prolog/v1.4.3";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-weave.follows = "cl-weave";
      inputs.paredit-cli.follows = "paredit-cli";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    cl-host-kit = {
      url = "github:nerima-lisp/cl-host-kit/v0.3.1";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-weave.follows = "cl-weave";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    cl-boundary-kit = {
      url = "github:nerima-lisp/cl-boundary-kit/v2.3.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-weave.follows = "cl-weave";
      inputs.cl-prolog.follows = "cl-prolog";
      inputs.cl-host-kit.follows = "cl-host-kit";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    cl-date-kit = {
      url = "github:nerima-lisp/cl-date-kit/v1.0.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };

    cl-concurrent-kit = {
      url = "github:nerima-lisp/cl-concurrent-kit/v0.6.1";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-boundary-kit.follows = "cl-boundary-kit";
      inputs.cl-date-kit.follows = "cl-date-kit";
      inputs.cl-weave.follows = "cl-weave";
      inputs.cl-nix-forge.follows = "cl-nix-forge";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      cl-nix-forge,
      cl-weave,
      paredit-cli,
      cl-boundary-kit,
      cl-concurrent-kit,
      cl-host-kit,
      treefmt-nix,
      ...
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-darwin"
      ];
    in
    cl-nix-forge.lib.${builtins.head systems}.mkPackageFlake {
      inherit self systems nixpkgs;

      pname = "cl-event-sourcing-kit";
      asd = ./cl-event-sourcing-kit.asd;
      root = ./.;

      meta = {
        description = "Domain-independent event sourcing protocol for Common Lisp";
        homepage = "https://github.com/nerima-lisp/cl-event-sourcing-kit";
        license = nixpkgs.lib.licenses.mit;
        platforms = nixpkgs.lib.platforms.unix;
      };

      lispDependencies = ctx: [
        cl-boundary-kit.packages.${ctx.system}.cl-boundary-kit
        cl-concurrent-kit.packages.${ctx.system}.cl-concurrent-kit
        cl-host-kit.packages.${ctx.system}.cl-host-kit
        cl-weave.packages.${ctx.system}.cl-weave
      ];

      lispCheckDependencies = ctx: [
        cl-weave.packages.${ctx.system}.cl-weave
      ];

      docs.root = ./docs;

      timeoutSeconds = 120;
      treefmt.evalModule = treefmt-nix.lib.evalModule;

      devShellPackages = ctx: [
        paredit-cli.packages.${ctx.system}.default
      ];

      extraOutputs =
        ctx:
        let
          coverage = ctx.cl.mkCoverageReport {
            drv = ctx.package;
            systems = [
              "cl-event-sourcing-kit"
              "cl-event-sourcing-kit/in-memory"
              "cl-event-sourcing-kit/projection"
              "cl-event-sourcing-kit/durable"
            ];
            timeoutSeconds = 120;
            killAfterSeconds = 30;
            name = "cl-event-sourcing-kit-coverage";
          };
        in
        {
          packages.coverage = coverage;
          checks.coverage = coverage;
          checks.coverage-strict = ctx.cl.mkScriptCheck {
            drv = ctx.package;
            entryPoint = "run-coverage.lisp";
            timeoutSeconds = 120;
            killAfterSeconds = 30;
            name = "cl-event-sourcing-kit-coverage-strict";
          };
        };
    };
}
