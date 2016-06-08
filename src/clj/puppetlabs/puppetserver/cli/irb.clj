(ns puppetlabs.puppetserver.cli.irb
  (:require [puppetlabs.puppetserver.cli.subcommand :as cli]
            [puppetlabs.services.jruby.jruby-core :as jruby-core]))

(defn irb-run!
  [config args]
  (jruby-core/cli-run! (jruby-core/initialize-config config) "irb" args))

(defn -main
  [& args]
  (cli/run irb-run! args))
