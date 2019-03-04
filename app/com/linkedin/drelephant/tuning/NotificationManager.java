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

package com.linkedin.drelephant.tuning;

import java.util.List;


/**
 * Defines set of methods which any class which are sending notification extends it.
 * Concrete implementation in EmailNotificationMgr
 */
public interface NotificationManager extends Manager {
  /**
   * Generate notification data within the given time range
   * @param startWindowTimeMS
   * @param endTimeWindowTimeMS
   * @return
   */
  List<NotificationData> generateNotificationData(long startWindowTimeMS, long endTimeWindowTimeMS);

  /**
   *  Send notification based on the notification data.
   * @param notificationData
   * @return
   */
  boolean sendNotification(List<NotificationData> notificationData);
}
