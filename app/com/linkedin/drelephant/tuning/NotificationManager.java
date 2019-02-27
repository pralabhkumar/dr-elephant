package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Manager;
import java.util.List;


public interface NotificationManager extends Manager {
  List<NotificationData> generateNotificationData(String source);
  boolean sendNotification(List<NotificationData> notificationData);
}
