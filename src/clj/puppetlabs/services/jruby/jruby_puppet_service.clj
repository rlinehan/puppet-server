(ns puppetlabs.services.jruby.jruby-puppet-service
  (:require [clojure.tools.logging :as log]
            [puppetlabs.services.jruby.jruby-puppet-core :as core]
            [puppetlabs.services.jruby.jruby-core :as jruby-core]
            [puppetlabs.services.jruby.jruby-agents :as jruby-agents]
            [puppetlabs.trapperkeeper.core :as trapperkeeper]
            [puppetlabs.trapperkeeper.services :as tk-services]
            [puppetlabs.services.protocols.jruby-puppet :as jruby]
            [slingshot.slingshot :as sling]
            [puppetlabs.services.jruby.jruby-schemas :as jruby-schemas]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

;; This service uses TK's normal config service instead of the
;; PuppetServerConfigService.  This is because that service depends on this one.

(trapperkeeper/defservice jruby-puppet-pooled-service
                          jruby/JRubyPuppetService
                          [[:ConfigService get-config]
                           [:ShutdownService shutdown-on-error]
                           [:PuppetProfilerService get-profiler]
                           [:PoolManagerService create-pool]]
  (init
    [this context]
    (let [config            (core/initialize-config (get-config))
          service-id        (tk-services/service-id this)
          agent-shutdown-fn (partial shutdown-on-error service-id)
          profiler          (get-profiler)]
      (core/verify-config-found! config)
      (log/info "Initializing the JRuby service")
      (if (:use-legacy-auth-conf config)
        (log/warn "The 'jruby-puppet.use-legacy-auth-conf' setting is set to"
                  "'true'.  Support for the legacy Puppet auth.conf file is"
                  "deprecated and will be removed in a future release.  Change"
                  "this setting to 'false' and migrate your authorization rule"
                  "definitions in the /etc/puppetlabs/puppet/auth.conf file to"
                  "the /etc/puppetlabs/puppetserver/conf.d/auth.conf file."))
      (core/add-facter-jar-to-system-classloader (:ruby-load-path config))
      (let [jruby-config (core/create-jruby-config config agent-shutdown-fn profiler)
            pool-context (create-pool jruby-config)]
        (-> context
            (assoc :pool-context pool-context)
            (assoc :borrow-timeout (:borrow-timeout config))
            (assoc :event-callbacks (atom []))
            (assoc :environment-class-info-tags (atom {}))))))
  (stop
   [this context]
   (let [{:keys [pool-context]} (tk-services/service-context this)
         on-complete (promise)]
     (log/debug "Beginning flush of JRuby pools for shutdown")
     (jruby-agents/send-flush-pool-for-shutdown! pool-context on-complete)
     @on-complete
     (log/debug "Finished flush of JRuby pools for shutdown"))
   context)

  (borrow-instance
    [this reason]
    (let [{:keys [pool-context borrow-timeout event-callbacks]} (tk-services/service-context this)]
      (jruby-core/borrow-from-pool-with-timeout pool-context reason @event-callbacks)))

  (return-instance
    [this jruby-instance reason]
    (let [event-callbacks (:event-callbacks (tk-services/service-context this))]
      (jruby-core/return-to-pool jruby-instance reason @event-callbacks)))

  (free-instance-count
    [this]
    (let [pool-context (:pool-context (tk-services/service-context this))
          pool         (jruby-core/get-pool pool-context)]
      (jruby-core/free-instance-count pool)))

  (mark-environment-expired!
    [this env-name]
    (let [{:keys [environment-class-info-tags pool-context]}
          (tk-services/service-context this)]
      (core/mark-environment-expired! pool-context
                                      env-name
                                      environment-class-info-tags)))

  (mark-all-environments-expired!
    [this]
    (let [{:keys [environment-class-info-tags pool-context]}
          (tk-services/service-context this)]
      (core/mark-all-environments-expired! pool-context
                                           environment-class-info-tags)))

  (get-environment-class-info
    [this jruby-instance env-name]
    (.getClassInfoForEnvironment jruby-instance env-name))

  (get-environment-class-info-tag
   [this env-name]
   (let [environment-class-info (:environment-class-info-tags
                                 (tk-services/service-context this))]
     (get-in @environment-class-info [env-name :tag])))

  (get-environment-class-info-cache-generation-id!
   [this env-name]
   (let [environment-class-info (:environment-class-info-tags
                                 (tk-services/service-context this))]
     (core/get-environment-class-info-cache-generation-id!
      environment-class-info
      env-name)))

  (set-environment-class-info-tag!
   [this env-name tag cache-generation-id-before-tag-computed]
   (let [environment-class-info (:environment-class-info-tags
                                 (tk-services/service-context this))]
     (swap! environment-class-info
            core/environment-class-info-cache-updated-with-tag
            env-name
            tag
            cache-generation-id-before-tag-computed)))

  (flush-jruby-pool!
   [this]
   (let [service-context (tk-services/service-context this)
          {:keys [pool-context]} service-context]
     (jruby-agents/send-flush-and-repopulate-pool! pool-context)))

  (get-pool-context
   [this]
   (:pool-context (tk-services/service-context this)))

  (register-event-handler
    [this callback-fn]
    (let [event-callbacks (:event-callbacks (tk-services/service-context this))]
      (swap! event-callbacks conj callback-fn))))

(def #^{:macro true
        :doc "An alias for the jruby-utils' `with-jruby-instance` macro so
             that it is accessible from the service namespace along with the
             rest of the API."}
  with-jruby-instance #'jruby-core/with-jruby-instance)

(def #^{:macro true
        :doc "An alias for the jruby-utils' `with-lock` macro so
             that it is accessible from the service namespace along with the
             rest of the API."}
  with-lock #'jruby-core/with-lock)
