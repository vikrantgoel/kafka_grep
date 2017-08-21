$(document).ready(function() {

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    $("#producer_reset").click(function() {
        $("#producer_bootstrap_server").val('');
        $("#producer_kafka_topic").val('');
        $("#producer_message").val('');
        $("#producer_messages").val('');
    });

    $("#consumer_reset").click(function() {
        $("#consumer_bootstrap_server").val('');
        $("#consumer_kafka_topic").val('');
        $("#consumer_group_id").val('');
        $("#consumer_timeout").val('');
        $("#consumer_offset").val('latest');
        $("#consumer_messages").val('');
    });

    $("#producer_submit").click(function() {

        var producerBootstrapServer = $("#producer_bootstrap_server").val() || "localhost:9092";
        var producerKafkaTopic = $("#producer_kafka_topic").val() || "smartpricing-aux-test";
        var producerMessage = $("#producer_message").val();
        console.log("Sending producer ajax call")
        $.ajax({
            url: '/produce',
            data: {producer_bootstrap_server: producerBootstrapServer,
                   producer_kafka_topic: producerKafkaTopic,
                   producer_message: producerMessage
                  },
            type: 'POST',
            success: function(response) {
                var responseString = JSON.stringify(response)
                $("#producer_messages").val($("#producer_messages").val() + responseString + "\n");
            },
            error: function(error) {
                $("#producer_messages").val(error);
            }
        });
    });

    $("#consumer_submit").click(async function() {

        var consumerBootstrapServer = $("#consumer_bootstrap_server").val() || "localhost:9092";
        var consumerKafkaTopic = $("#consumer_kafka_topic").val() || "smartpricing-aux-test";
        var consumerOffset = $("#consumer_offset").val();
        var consumerTimeout = parseInt($("#consumer_timeout").val() || "10");
        var consumerGroupId = $("#consumer_group_id").val() || new Date().getTime();

        if (consumerTimeout < 1) {
            consumerTimeout = 1
        } else if (consumerTimeout > 30) {
            consumerTimeout = 30
        }

        var timeoutEnd = new Date().getTime() + (consumerTimeout * 1000);
        while (new Date().getTime() < timeoutEnd) {

            // Non blocking
            $.ajax({
                url: '/consume',
                type: 'POST',
                data: {consumer_bootstrap_server: consumerBootstrapServer,
                       consumer_kafka_topic: consumerKafkaTopic,
                       consumer_offset: consumerOffset,
                       consumer_timeout: consumerTimeout,
                       consumer_group_id: consumerGroupId
                      },
                success: function(response) {
                    console.log(response);
                    if(JSON.stringify(response.consumer_messages) !== "null") {
                        $("#consumer_messages").val($("#consumer_messages").val() + JSON.stringify(response.consumer_messages) + "\n");
                        timeoutEnd = new Date().getTime() + (consumerTimeout * 1000);
                    }
                },
                error: function(error) {
                    $("#consumer_messages").val(error);
                },
                timeout: 4000
            });
            await sleep(5000);
        }
        console.log("Timeout")
    });



});