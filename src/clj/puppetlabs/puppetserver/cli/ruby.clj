(ns puppetlabs.puppetserver.cli.ruby
  (:require [puppetlabs.puppetserver.cli.subcommand :as cli]
            [puppetlabs.services.jruby.jruby-core :as jruby-core]))

(defn ruby-run!
  [config args]
  (jruby-core/cli-ruby! (jruby-core/initialize-config config) args))

(defn -main
  [& args]
  (cli/run ruby-run! args))
