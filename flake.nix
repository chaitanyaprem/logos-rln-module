{
  description = "Logos RLN Membership Management Module";

  nixConfig = {
    extra-substituters = [ "https://cache.nix.logos.co/public" ];
    extra-trusted-public-keys = [
      "public:zaDAOkd+E3cQyFm9J7qejQYd9HoLJc1SJvnk68hNWeI="
    ];
  };

  inputs = {
    logos-module-builder.url = "github:logos-co/logos-module-builder";
    rln_lez_module.url = "github:logos-co/logos-rln-lez-module";
    membership_allocation_client_module.url = "github:logos-co/logos-membership-allocation-client-module";
  };

  outputs = inputs@{ self, logos-module-builder, ... }:
    let
      nixpkgs = logos-module-builder.inputs.nixpkgs;
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" "x86_64-windows" ];
      forAllSystems = fn: nixpkgs.lib.genAttrs systems fn;

      module = logos-module-builder.lib.mkLogosModule {
        src = ./.;
        configFile = ./metadata.json;
        flakeInputs = inputs;
      };
    in
    {
      packages = forAllSystems (system:
        let
          m = module.packages.${system};
          pkgs = import nixpkgs { inherit system; };
          rlnSignal = pkgs.rustPlatform.buildRustPackage {
            pname = "rln-signal";
            version = "0.1.0";
            src = ./rust-lib/rln-signal;
            cargoLock.lockFile = ./rust-lib/rln-signal/Cargo.lock;
            doCheck = false;
            postInstall = ''
              install -Dm644 include/rln_signal.h $out/include/rln_signal.h
            '';
          };
        in m // {
          rln_module = m.default;
        } // nixpkgs.lib.optionalAttrs (system != "x86_64-windows") {
          rln-signal = rlnSignal;
        });

      inherit (module) config;

      apps = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          lidlGen = logos-module-builder.inputs.logos-rust-sdk.packages.${system}.lidl-gen;
          sdkSrc = logos-module-builder.packages.${system}.rust-sdk-src;
          protocolVersion =
            let
              header = builtins.readFile
                "${logos-module-builder.inputs.logos-protocol}/cpp/logos_protocol.h";
              parts = builtins.split "LOGOS_PROTOCOL_VERSION_STRING \"([^\"]*)\"" header;
            in
            if builtins.length parts < 2
            then throw "logos-protocol header carries no LOGOS_PROTOCOL_VERSION_STRING"
            else builtins.head (builtins.elemAt parts 1);
          generate = pkgs.writeShellApplication {
            name = "rln-module-generate";
            runtimeInputs = [ lidlGen pkgs.gitMinimal ];
            text = ''
              root="''${1:-$(git rev-parse --show-toplevel)}"
              echo "generating rust-lib/generated/provider_gen.rs (protocol ${protocolVersion}) ..."
              mkdir -p "$root/rust-lib/generated"
              logos-lidl-gen "$root/rust-lib/rln_module.lidl" --provider \
                --concurrency multi \
                --protocol-version ${protocolVersion} \
                --dep rln_lez_module="$root/rust-lib/deps/rln_lez_module.lidl" \
                --dep membership_allocation_client_module="$root/rust-lib/deps/membership_allocation_client_module.lidl" \
                -o "$root/rust-lib/generated/provider_gen.rs"
              echo "staging the SDK source at logos-rust-sdk-src/ ..."
              rm -rf "''${root:?}/logos-rust-sdk-src"
              cp -RL "${sdkSrc}" "$root/logos-rust-sdk-src"
              chmod -R u+w "$root/logos-rust-sdk-src"
              echo "done. bare 'cargo build' now works in rust-lib/"
            '';
          };
        in {
          generate = {
            type = "app";
            program = "${generate}/bin/rln-module-generate";
          };
        });
    };
}
