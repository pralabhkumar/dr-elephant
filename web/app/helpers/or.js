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

/**
 * helper takes boolean array and returns true if one or more elements are true else returns false
 * @param params The boolean array for the helper
 * @returns {boolean}
 */

export function or(params) {
  var result = false;
  for (var i = 0; i < params.length; i++) {
    result = result || !!params[i];
  }
  return result;
}
export default Ember.Helper.helper(or);