"use strict";

const azure = require('azure-storage');
const config = require('config');
const logger = require('siteshot-logger');

const queueSvc = azure.createQueueService(config.Azure.Connection.AccountName, config.Azure.Connection.AccountKey);

/**
 * Sets up queues in Azure if they don't already exist
 * @constructor
 */
exports.SetupQueues = function () {
    for (var queue in config.Azure.Queues) {
        queueSvc.createQueueIfNotExists(config.Azure.Queues[queue], function (err, res) {
            logger.debug("Created", res.name, "queue");
        });
    }
};

/**
 * Pushes a job to the Distributor queue
 * @param jobContract Object containing job details
 * @param callback
 * @constructor
 */
exports.IssueJob = function (jobContract, callback) {
    queueSvc.createMessage(config.Azure.Queues.Distributor, JSON.stringify(jobContract), callback);
};

/**
 * Pushes an item out to the scans queue
 * @param jobContract Object containing job details
 * @param callback
 * @constructor
 */
exports.PublishToScanQueue = function (jobContract, callback) {
    queueSvc.createMessage(config.Azure.Queues.Scans, JSON.stringify(jobContract), callback);
};


/**
 * Checks the queue for jobs
 * @param processJobCallback function to call when a job is found
 * @param noJobCallback function to call when there's no job
 * @constructor
 */
exports.CheckForJobs = function (processJobCallback, noJobCallback) {
    function ProcessJob(message, callback) {
        function _deleteMessage(delMessage) {
            queueSvc.deleteMessage(config.Azure.Queues.Distributor, delMessage.messageId, delMessage.popReceipt, function (error, response) {
                if (!error) {
                    logger.debug(delMessage.messageId, "deleted");
                }
            });
        }

        message.job = JSON.parse(message.messageText);

        return callback(message, _deleteMessage);
    }

    queueSvc.getMessages(config.Azure.Queues.Distributor, {numOfMessages: 20}, function (error, result, response) {
            if (!error && result.length > 0) {
                logger.debug("Have", result.length, "jobs");
                for (var i = 0; i < result.length; i++) {
                    ProcessJob(result[i], processJobCallback);
                }
            } else {
                return noJobCallback(false);
            }
        }
    );
};

/**
 * Checks the queue for updates
 * @param processJobCallback function to call when there is a job
 * @param noJobCallback function to call when there's no job
 * @constructor
 */
exports.CheckForUpdateJobs = function (processJobCallback, noJobCallback) {
    function ProcessJob(message, callback) {
        function _deleteMessage(delMessage) {
            queueSvc.deleteMessage(config.Azure.Queues.Updates, delMessage.messageId, delMessage.popReceipt, function (error, response) {
                if (!error) {
                    logger.debug(delMessage.messageId, "deleted");
                } else {
                    logger.debug("Error deleting job", error);
                }
            });
        }

        message.job = JSON.parse(message.messageText);

        return callback(message, _deleteMessage);
    }

    queueSvc.getMessages(config.Azure.Queues.Updates, {numOfMessages: 20}, function (error, result, response) {
            if (!error && result.length > 0) {
                logger.debug("Have", result.length, "jobs");

                for (var i = 0; i < result.length; i++) {
                    ProcessJob(result[i], processJobCallback);
                }
            } else {
                return noJobCallback(false);
            }
        }
    );
};