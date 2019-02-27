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


public class EmailNotificationManager implements NotificationManager {
  private static final Logger logger = Logger.getLogger(EmailNotificationManager.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private long startWindowTimeMS = 0;
  private long endTimeWindowTimeMS = 0;
  private List<NotificationData> notificationMessages = null;
  private static final String MAIL_HOST = "mail.host";
  private static final String MAIL_HOST_LINKEDIN = "mail-gw.corp.linkedin.com";
  private static final String DEFAULT_DOMAIN = "@linkedin.com";

  @Override
  public List<NotificationData> generateNotificationData(String source) {
    logger.info(" Generating Notification ");
    notificationMessages = new ArrayList<NotificationData>();
    generateDevelopersNotificationData();
    generateStakeHolderNotificationData();
    logger.info(" Notification messages  " + notificationMessages.size());
    return notificationMessages;
  }

  private void generateDevelopersNotificationData() {
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        //.between(JobSuggestedParamSet.TABLE.updatedTs, startWindowTimeMS, endTimeWindowTimeMS)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, true)
        .eq(JobSuggestedParamSet.TABLE.fitness, 10000)
        .findList();
    if (jobSuggestedParamSets.size() > 0) {
      NotificationData data = new NotificationData("pkumar2@linkedin.com");
      data.setSubject(" Following jobs have penalty parameter as the best parmeter . Please fix this");
      for (JobSuggestedParamSet jobSuggestedParamSet : jobSuggestedParamSets) {
        data.addContent(jobSuggestedParamSet.toString());
      }
      notificationMessages.add(data);
    }
    logger.info("Developer Notification "+jobSuggestedParamSets.size());
  }

  private void generateStakeHolderNotificationData() {
    List<TuningJobDefinition> tuningJobDefinitions = TuningJobDefinition.find.select("*")
        .where()
        .between(TuningJobDefinition.TABLE.updatedTs, startWindowTimeMS, endTimeWindowTimeMS)
        .eq(TuningJobDefinition.TABLE.autoApply, true)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, false)
        .findList();
    if (tuningJobDefinitions.size() > 0) {
      NotificationData data = new NotificationData("pkumar2@linkedin.com");
      data.setSubject(" Following jobs are tunned ");
      for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
        data.addContent(tuningJobDefinition.job.jobDefId);
      }
    }
    logger.info("Developer Notification "+tuningJobDefinitions.size());
  }

  @Override
  public boolean sendNotification(List<NotificationData> notificationDatas) {
    try {
      Properties props = new Properties();
      props.put(MAIL_HOST, MAIL_HOST_LINKEDIN);
      Session session = Session.getDefaultInstance(props);
      for (NotificationData notificationData : notificationDatas) {
        Message message = new MimeMessage(session);
        InternetAddress fromAddress = new InternetAddress("pkumar2@linkedin.com");
        message.setFrom(fromAddress);
        InternetAddress addressInternet = new InternetAddress(notificationData.getSenderAddress());
        InternetAddress[] address = {addressInternet};
        message.setRecipients(Message.RecipientType.TO, address);
        message.setSubject(notificationData.getSubject());
        MimeMultipart multipart = new MimeMultipart("related");

        BodyPart messageBodyPart = new MimeBodyPart();
        StringBuffer content = new StringBuffer();
        for (String data : notificationData.getContent()) {
          content.append(data).append("\n");
        }
        messageBodyPart.setContent(content, "text/html; charset=utf-8");
        multipart.addBodyPart(messageBodyPart);
        message.setContent(multipart);
        Transport.send(message);
        logger.info("Sending email to recipients");
      }
    } catch (MessagingException messageException) {
      logger.error(" Exception while sending messages " + messageException);
      return false;
    }
    return true;
  }


  @Override
  public boolean execute() {
    startWindowTimeMS = endTimeWindowTimeMS == 0 ? System.currentTimeMillis() - 3600000 : endTimeWindowTimeMS + 1;
    endTimeWindowTimeMS = System.currentTimeMillis();
    List<NotificationData> notifications = generateNotificationData("tuneIn");
    sendNotification(notifications);
    return false;
  }

  @Override
  public String getManagerName() {
    return null;
  }
}
