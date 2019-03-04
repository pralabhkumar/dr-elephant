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
package com.linkedin.drelephant.tuning.alerting;

/**
 * Constant class for alerting framework. These constants are generally shared across
 * classes.
 */
public final class Constant {
  static String SUBJECT_HEADER = "TuneIn Notification";
  static String MESSAGE_HEADER_TEMPLATE = "<h3> Hi {0} </h3>" + "<h3> Below are the notification from TuneIn </h3> ";
  static String MESSAGE_SUBJECT_TEMPLATE = "<h4>{0}</h4>";
  static String MESSAGE_BODY_TEMPLATE = "{0})  <i>{1}</i><br>";
  static String MESSAGE_FOOTER_TEMPLATE = "<h3>Regards <br> TuneIn";
  static final String DELIMITER_BETWEEN_USERNAME_EMAIL = "@";
  static final String ALERTING_ENABLED_PROPERTY = "alerting.enabled";
  static final String ALERTING_EMAIL_HOST_PROPERTY="alerting.mail.host";
  static final String ALERTING_FROM_EMAIL_ADDRESS_PROPERTY = "alerting.email.address";
  static final String ALERTING_DEVELOPERS_RECIPIENT_ADDRESS_PROPERTY = "alerting.developers.email.address";
  static final String EMAIL_DOMAIN_NAME_PROPERTY = "alerting.domain.name";

  public enum NotificationType {DEVELOPER, STAKEHOLDER}
  public enum SenderEmailSchema {USERNAME,EMAIL_DOMAIN_NAME}

}
