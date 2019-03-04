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

import static com.linkedin.drelephant.tuning.alerting.Constant.*;

import java.util.ArrayList;
import java.util.List;


/**
 * Represents notification data .
 */
public class NotificationData {
  enum MessagePriority {LOW, HIGH}

  private String subject;
  private List<String> content;
  private String senderAddress;

  private NotificationType notificationType;

  public NotificationData(String senderAddress) {
    this.senderAddress = senderAddress;
    content = new ArrayList<String>();
  }

  public void addContent(String data) {
    this.content.add(data);
  }

  public String getSenderAddress() {
    return this.senderAddress;
  }

  public List<String> getContent() {
    return this.content;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  NotificationType getNotificationType() {
    return notificationType;
  }

  public void setNotificationType(NotificationType notificationType) {
    this.notificationType = notificationType;
  }
}
