(ns puppetlabs.puppetserver.cli.gem
  (:require [puppetlabs.puppetserver.cli.subcommand :as cli]
            [puppetlabs.services.jruby.jruby-core :as jruby-core]))

(defn gem-run!
  [config args]
  (jruby-core/cli-run! (jruby-core/initialize-config config) "gem" args))

(defn -main
  [& args]
  (cli/run gem-run! args))
