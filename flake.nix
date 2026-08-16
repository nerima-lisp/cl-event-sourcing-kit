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

    cl-resilience-kit = {
      url = "github:nerima-lisp/cl-resilience-kit/v1.0.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.cl-boundary-kit.follows = "cl-boundary-kit";
      inputs.cl-concurrent-kit.follows = "cl-concurrent-kit";
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
      cl-resilience-kit,
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
        cl-resilience-kit.packages.${ctx.system}.cl-resilience-kit
        cl-host-kit.packages.${ctx.system}.cl-host-kit
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
          normalize-coverage-report =
            drv:
            drv.overrideAttrs (old: {
              nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ nixpkgs.legacyPackages.${ctx.system}.perl ];
              postInstall = (old.postInstall or "") + ''
                perl - "$out" <<'PERL'
                use strict;
                use warnings;

                my ($directory) = @ARGV;
                die "coverage output directory is missing\n" unless defined $directory;
                opendir(my $handle, $directory) or die "cannot read coverage output: $!\n";
                my (%renames, %targets);
                while (my $name = readdir($handle)) {
                  next unless $name =~ /\.html\z/ && $name ne "cover-index.html";
                  my $path = "$directory/$name";
                  open(my $input, "<", $path) or die "cannot read $path: $!\n";
                  local $/;
                  my $contents = <$input>;
                  close($input);
                  my ($source) = $contents =~ m{Coverage report: (.+?) <br />};
                  die "coverage report source is missing in $path\n" unless defined $source;
                  my $relative = $source;
                  $relative =~ s{^.*?/source/}{};
                  die "coverage report source is not rooted at source: $path\n"
                    if $relative eq $source;
                  $relative =~ s{/}{--}g;
                  $relative =~ s{[^A-Za-z0-9._-]}{_}g;
                  my $target = "$relative.html";
                  die "duplicate coverage report target: $target\n"
                    if exists $targets{$target};
                  $contents =~ s{/nix/var/nix/builds/[^<\s']+/source/}{/source/}g;
                  open(my $output, ">", $path) or die "cannot write $path: $!\n";
                  print {$output} $contents;
                  close($output);
                  $renames{$name} = $target;
                  $targets{$target} = 1;
                }
                closedir($handle);
                die "coverage report contains no source files\n" unless %renames;

                my $index_path = "$directory/cover-index.html";
                open(my $index_input, "<", $index_path)
                  or die "cannot read $index_path: $!\n";
                local $/;
                my $index = <$index_input>;
                close($index_input);
                $index =~ s{href=(['"])([^'"]+\.html)\1}{
                  my ($quote, $link) = ($1, $2);
                  die "coverage report link has no target: $link\n"
                    unless exists $renames{$link};
                  "href=$quote$renames{$link}$quote";
                }ge;
                $index =~ s{/nix/var/nix/builds/[^<\s']+/source/}{/source/}g;
                for my $name (keys %renames) {
                  die "coverage report link was not rewritten: $name\n"
                    if $index =~ /\Q$name\E/;
                }
                open(my $index_output, ">", $index_path)
                  or die "cannot write $index_path: $!\n";
                print {$index_output} $index;
                close($index_output);

                for my $name (keys %renames) {
                  rename "$directory/$name", "$directory/$renames{$name}"
                    or die "cannot rename coverage report $name: $!\n";
                }
                die "transient build path remains in coverage report\n"
                  if $index =~ m{/nix/var/nix/builds/};
                PERL
              '';
            });
          coverage = normalize-coverage-report (
            (ctx.cl.mkCoverageReport {
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
            }).overrideAttrs
              (old: {
                postInstall = (old.postInstall or "") + ''
                  find "$out" -type f -name '*.fasl' -delete
                '';
              })
          );
        in
        {
          packages.coverage = coverage;
          checks.coverage = coverage;
          checks.coverage-strict =
            (ctx.cl.mkScriptCheck {
              drv = ctx.package;
              entryPoint = "run-coverage.lisp";
              timeoutSeconds = 120;
              killAfterSeconds = 30;
              name = "cl-event-sourcing-kit-coverage-strict";
            }).overrideAttrs
              (old: {
                postInstall = (old.postInstall or "") + ''
                  find "$out" -type f -name '*.fasl' -delete
                '';
              });
        };

      overrideOutputs =
        ctx:
        let
          without-fasl =
            drv:
            drv.overrideAttrs (old: {
              postInstall = (old.postInstall or "") + ''
                find "$out" -type f -name '*.fasl' -delete
              '';
            });
        in
        {
          packages.default = without-fasl ctx.generated.packages.default;
          packages.cl-event-sourcing-kit = without-fasl ctx.generated.packages.cl-event-sourcing-kit;
          checks.default = without-fasl ctx.generated.checks.default;
        };
    };
}
