/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import Ember from 'ember';

export default Ember.Route.extend({
  ajax: Ember.inject.service(),

  beforeModel: function (transition) {
    this.jobid = transition.queryParams.jobid;
  },
  model(){
    return Ember.RSVP.hash({
      jobs:   this.store.queryRecord('job', {jobid: this.get("jobid")}),
      tunein: this.store.queryRecord('tunein', {id: this.get("jobid")})
    });
  },
    setupController: function(controller, model) {
      controller.set('model', model);
      controller.set('currentAlgorithm', model.tunein.get('tuningAlgorithm')),
      controller.set('currentIterationCount', model.tunein.get('iterationCount'))
  },
  actions: {
    paramChange(tunein) {
      console.log(this.get('model.jobs'))
      return this.get('ajax').post('/rest/tunein', {
        contentType: 'application/json',
        data: JSON.stringify({
          tunein: tunein
        })
      })
    },
    error(error, transition) {
      if (error.errors[0].status == 404) {
        return this.transitionTo('not-found', { queryParams: {'previous': window.location.href}});
      }
    }
  }
});
