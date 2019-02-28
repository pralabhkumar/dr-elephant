package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.alerting.EmailNotificationManager;
import java.sql.Timestamp;
import java.util.List;
import models.JobSuggestedParamSet;
import com.linkedin.drelephant.ElephantContext;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;

import org.apache.hadoop.conf.Configuration;


public class AlertingTest implements Runnable {
  private void populateTestData() {
    try {
      initParamGenerater();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testAlerting();
  }

  private void testAlerting() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis() + 1000;
    assertTrue(" Alerting is not enabled . So generate data should be null ",
        new EmailNotificationManager(configuration).generateNotificationData(startTime, endTime) == null);

    configuration.setBoolean("alerting.enabled", true);

    List<NotificationData> notificationData =
        new EmailNotificationManager(configuration).generateNotificationData(startTime, endTime);

    assertTrue("No data within the range provided ", notificationData.size() == 0);



    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*").where().findUnique();
    jobSuggestedParamSet.updatedTs = new Timestamp(startTime + 100);
    jobSuggestedParamSet.update();

    


    NotificationManager manager = new EmailNotificationManager(configuration);

    List<NotificationData> notificationDataAfterUpdate = manager.generateNotificationData(startTime, endTime);

    assertTrue(" Notification data size ", notificationDataAfterUpdate.size() == 1);
    assertTrue(" Developers Notification  ",
        notificationDataAfterUpdate.get(0).getNotificationType().equals("developer"));

    /**
     * If user want to test email functionality
     */
    //assertTrue(" Email send successfully ", manager.sendNotification(notificationDataAfterUpdate));
  }
}
