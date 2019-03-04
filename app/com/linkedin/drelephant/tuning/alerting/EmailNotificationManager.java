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

import com.linkedin.drelephant.tuning.NotificationData;
import com.linkedin.drelephant.tuning.NotificationManager;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import static com.linkedin.drelephant.tuning.alerting.Constant.*;


/**
 * This class will be used to send email notifications
 * There are two types of notifications
 * 1) Developers : Notification will go to developers
 * 2) StakeHolder : Notification will go to the user whoes jobs are onboarded onto
 * cluster
 */
public class EmailNotificationManager implements NotificationManager {
  private static final Logger logger = Logger.getLogger(EmailNotificationManager.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private static boolean isAlertingEnabled = false;
  private static final String MAIL_HOST = "mail.host";
  private static String MAIL_HOST_URL = null;
  private static String FROM_EMAIL_ADDRESS = null;
  private long endTimeWindowTimeMS = 0;
  private Configuration configuration = null;

  public EmailNotificationManager(Configuration configuration) {
    this.configuration = configuration;
    //todo : Creating configuration builder and with information about each property
    isAlertingEnabled = configuration.getBoolean(ALERTING_ENABLED_PROPERTY, false);
    MAIL_HOST_URL = configuration.get(ALERTING_EMAIL_HOST_PROPERTY);
    FROM_EMAIL_ADDRESS = configuration.get(ALERTING_FROM_EMAIL_ADDRESS_PROPERTY);
  }

  /**
   * Entry point , or main method which is called to execute this class.
   * @return
   */
  @Override
  public boolean execute() {
    try {
      long startWindowTimeMS =
          endTimeWindowTimeMS == 0 ? System.currentTimeMillis() - 3600000 : endTimeWindowTimeMS + 1;
      endTimeWindowTimeMS = System.currentTimeMillis();
      List<NotificationData> notifications = generateNotificationData(startWindowTimeMS, endTimeWindowTimeMS);
      sendNotification(notifications);
    } catch (Exception e) {
      logger.error(" Exception occurred while generating alerts ", e);
      return false;
    }
    return true;
  }

  /**
   * Generate Notification data for the window .
   * @param startWindowTimeMS
   * @param endTimeWindowTimeMS
   * @return
   */
  @Override
  public List<NotificationData> generateNotificationData(long startWindowTimeMS, long endTimeWindowTimeMS) {
    if (!arePrerequisteMatch()) {
      return null;
    }
    logger.debug(" Generating Notification ");
    NotificationDataGenerator notificationDataGenerator =
        new NotificationDataGenerator(startWindowTimeMS, endTimeWindowTimeMS, configuration);
    return notificationDataGenerator.generateNotificationData();
  }

  private boolean arePrerequisteMatch() {
    if (!isAlertingEnabled) {
      logger.info(" Alerting is not enabled . Hence no point generating notification data ");
      return false;
    }
    if (MAIL_HOST_URL == null) {
      logger.error(" Mail host is not provided . Hence cannot send email");
      return false;
    }
    if (FROM_EMAIL_ADDRESS == null) {
      logger.error(" From email address is not set . Hence cannot send email ");
      return false;
    }
    return true;
  }

  /**
   * Sending/Emailing the notifications
   * @param notificationDatas
   * @return
   */
  @Override
  public boolean sendNotification(List<NotificationData> notificationDatas) {
    try {
      if (notificationDatas == null || notificationDatas.size() == 0) {
        logger.info(" No notification to send ");
        return false;
      } else {
        Properties props = new Properties();
        props.put(MAIL_HOST, MAIL_HOST_URL);
        Session session = Session.getDefaultInstance(props);
        InternetAddress fromAddress = new InternetAddress(FROM_EMAIL_ADDRESS);
        Map<String, List<NotificationData>> notificationsBySenders =
            mergeNotificationBasedOnSendersAddress(notificationDatas);
        return sendEmail(session, fromAddress, notificationsBySenders);
      }
    } catch (Exception e) {
      logger.error(" Exception while sending notification ", e);
      return false;
    }
  }

  /**
   * Merge all the notification based on the To email address . It will make sure
   * only one email will be send to the intended person
   * @param notificationDatas
   * @return
   */
  private Map<String, List<NotificationData>> mergeNotificationBasedOnSendersAddress(
      List<NotificationData> notificationDatas) {
    Map<String, List<NotificationData>> messagedNotificationData = new HashMap<String, List<NotificationData>>();
    for (NotificationData notificationData : notificationDatas) {
      List<NotificationData> notificationsBySender;
      if (messagedNotificationData.containsKey(notificationData.getSenderAddress())) {
        notificationsBySender = messagedNotificationData.get(notificationData.getSenderAddress());
      } else {
        notificationsBySender = new ArrayList<NotificationData>();
      }
      notificationsBySender.add(notificationData);
      messagedNotificationData.put(notificationData.getSenderAddress(), notificationsBySender);
    }
    return messagedNotificationData;
  }

  private boolean sendEmail(Session session, InternetAddress fromAddress,
      Map<String, List<NotificationData>> notificationsBySenders) {
    boolean hasExceptionOccurred = false;
    for (String senderAddress : notificationsBySenders.keySet()) {
      try {
        Message message = new MimeMessage(session);
        message.setFrom(fromAddress);
        InternetAddress addressInternet = new InternetAddress(senderAddress);
        InternetAddress[] address = {addressInternet};
        message.setRecipients(Message.RecipientType.TO, address);
        message.setSubject(SUBJECT_HEADER);
        StringBuilder messageData = new StringBuilder();
        String headerOfMessage = MessageFormat.format(MESSAGE_HEADER_TEMPLATE,
            senderAddress.split(DELIMITER_BETWEEN_USERNAME_EMAIL)[SenderEmailSchema.USERNAME.ordinal()]);
        messageData.append(headerOfMessage);
        for (NotificationData notificationData : notificationsBySenders.get(senderAddress)) {
          messageData.append(MessageFormat.format(MESSAGE_SUBJECT_TEMPLATE, notificationData.getSubject()));
          appendBodyOfMessage(notificationData.getContent(), messageData);
        }
        messageData.append(MESSAGE_FOOTER_TEMPLATE);
        MimeMultipart multipart = new MimeMultipart("related");
        BodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setContent(messageData.toString(), "text/html; charset=utf-8");
        multipart.addBodyPart(messageBodyPart);
        message.setContent(multipart);
        Transport.send(message);
        logger.info("Sending email to recipients " + senderAddress);
      } catch (Exception messageException) {
        logger.error(" Exception while sending message " + messageException);
        hasExceptionOccurred = true;
      }
    }
    if (hasExceptionOccurred) {
      logger.warn(" At least one exception have occurred while sending messages ");
      return false;
    } else {
      return true;
    }
  }

  private void appendBodyOfMessage(List<String> notificationContent, StringBuilder messageData) {
    int count = 0;
    for (String data : notificationContent) {
      count++;
      messageData.append(MessageFormat.format(MESSAGE_BODY_TEMPLATE, count, data));
    }
  }

  @Override
  public String getManagerName() {
    return "EmailNotificationManager";
  }
}
