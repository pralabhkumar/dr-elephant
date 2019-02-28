package com.linkedin.drelephant.tuning.alerting;

import com.linkedin.drelephant.tuning.NotificationData;
import com.linkedin.drelephant.tuning.NotificationManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningJobDefinition;
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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;


public class EmailNotificationManager implements NotificationManager {
  private static final Logger logger = Logger.getLogger(EmailNotificationManager.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private List<NotificationData> notificationMessages = null;
  private static boolean isAlertingEnabled = false;
  private static final String MAIL_HOST = "mail.host";
  private static String MAIL_HOST_URL = null;
  private static String FROM_EMAIL_ADDRESS = null;
  private long startWindowTimeMS = 0;
  private long endTimeWindowTimeMS = 0;
  private static String DEVELOPERS_EMAIL_ADDRESS = null;
  private static String EMAIL_DOMAIN_NAME = null;

  public EmailNotificationManager(Configuration configuration) {
    isAlertingEnabled = configuration.getBoolean("alerting.enabled", false);
    MAIL_HOST_URL = configuration.get("alerting.mail.host");
    FROM_EMAIL_ADDRESS = configuration.get("alerting.email.address");
    DEVELOPERS_EMAIL_ADDRESS = configuration.get("alerting.developers.email.address");
    EMAIL_DOMAIN_NAME = configuration.get("alerting.domain.name");
  }

  @Override
  public List<NotificationData> generateNotificationData(long startWindowTimeMS, long endTimeWindowTimeMS) {
    if (!arePrerequisteMatch()) {
      return null;
    }
    logger.info(" Generating Notification ");
    notificationMessages = new ArrayList<NotificationData>();
    generateDevelopersNotificationData(startWindowTimeMS, endTimeWindowTimeMS);
    generateStakeHolderNotificationData(startWindowTimeMS, endTimeWindowTimeMS);
    logger.info(" Notification messages  " + notificationMessages.size());
    return notificationMessages;
  }

  private void generateDevelopersNotificationData(long startWindowTimeMS, long endTimeWindowTimeMS) {
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .between(JobSuggestedParamSet.TABLE.updatedTs, new Timestamp(startWindowTimeMS),
            new Timestamp(endTimeWindowTimeMS))
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, true)
        .eq(JobSuggestedParamSet.TABLE.fitness, 10000)
        .findList();
    if (jobSuggestedParamSets.size() > 0) {
      NotificationData data = new NotificationData(DEVELOPERS_EMAIL_ADDRESS);
      data.setSubject(" Following jobs have penalty parameter as the best parmeter . Please fix this");
      data.setNotificationType("developer");
      for (JobSuggestedParamSet jobSuggestedParamSet : jobSuggestedParamSets) {
        data.addContent(jobSuggestedParamSet.toString());
      }
      notificationMessages.add(data);
    }
    logger.info("Developer Notification " + jobSuggestedParamSets.size());
  }

  private void generateStakeHolderNotificationData(long startWindowTimeMS, long endTimeWindowTimeMS) {
    List<TuningJobDefinition> tuningJobDefinitions = TuningJobDefinition.find.select("*")
        .where()
        .between(TuningJobDefinition.TABLE.updatedTs, startWindowTimeMS, endTimeWindowTimeMS)
        .eq(TuningJobDefinition.TABLE.autoApply, true)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, false)
        .findList();
    if (tuningJobDefinitions.size() > 0) {
      NotificationData data = new NotificationData(DEVELOPERS_EMAIL_ADDRESS);
      data.setSubject(" Following jobs are tunned ");
      data.setNotificationType("stakeholder");
      for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
        data.addContent(tuningJobDefinition.job.jobDefId);
      }
    }
    logger.info("StakeHolder Notification " + tuningJobDefinitions.size());
  }

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
        InternetAddress fromAddress = new InternetAddress(DEVELOPERS_EMAIL_ADDRESS);
        return sendEmail(session, fromAddress, notificationDatas);
      }
    } catch (Exception e) {
      logger.error(" Exception while sending notification ", e);
      return false;
    }
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

  private boolean sendEmail(Session session, InternetAddress fromAddress, List<NotificationData> notificationDatas) {
    try {
      for (NotificationData notificationData : notificationDatas) {
        Message message = new MimeMessage(session);
        message.setFrom(fromAddress);
        InternetAddress addressInternet = new InternetAddress(notificationData.getSenderAddress());
        InternetAddress[] address = {addressInternet};
        message.setRecipients(Message.RecipientType.TO, address);
        message.setSubject(notificationData.getSubject());
        createBodyOfMessage(message, notificationData);
        Transport.send(message);
        logger.info("Sending email to recipients " + notificationData.getSenderAddress());
      }
    } catch (MessagingException messageException) {
      logger.error(" Exception while sending messages " + messageException);
      return false;
    }
    return true;
  }

  private void createBodyOfMessage(Message message, NotificationData notificationData) throws MessagingException {
    MimeMultipart multipart = new MimeMultipart("related");
    BodyPart messageBodyPart = new MimeBodyPart();
    StringBuilder content = new StringBuilder();
    for (String data : notificationData.getContent()) {
      content.append(data).append("\n");
    }
    messageBodyPart.setContent(content.toString(), "text/html; charset=utf-8");
    multipart.addBodyPart(messageBodyPart);
    message.setContent(multipart);
  }

  @Override
  public boolean execute() {
    startWindowTimeMS = endTimeWindowTimeMS == 0 ? System.currentTimeMillis() - 3600000 : endTimeWindowTimeMS + 1;
    endTimeWindowTimeMS = System.currentTimeMillis();
    List<NotificationData> notifications = generateNotificationData(startWindowTimeMS, endTimeWindowTimeMS);
    sendNotification(notifications);
    return false;
  }

  @Override
  public String getManagerName() {
    return null;
  }
}
