$(document).ready(function() {

    $("#producer_reset").click(function() {
        $("#producer_bootstrap_server").val('');
        $("#producer_kafka_topic").val('');
        $("#producer_message").val('');
        $("#producer_messages").val('');
    });

    $("#consumer_reset").click(function() {
        $("#consumer_bootstrap_server").val('');
        $("#consumer_kafka_topic").val('');
        $("#consumer_timeout").val('');
        $("#consumer_offset").val('latest');
        $("#consumer_messages").val('');
    });

    $("#producer_submit").click(function() {

        var producerBootstrapServer = $("#producer_bootstrap_server").val();
        var producerKafkaTopic = $("#producer_kafka_topic").val();
        var producerMessage = $("#producer_message").val();

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

    $("#consumer_submit").click(function() {

        var consumerBootstrapServer = $("#consumer_bootstrap_server").val();
        var consumerKafkaTopic = $("#consumer_kafka_topic").val();
        var consumerOffset = $("#consumer_offset").val();
        var consumerTimeout = $("#consumer_timeout").val() || "0";
        $.ajax({
            url: '/consume',
            data: {consumer_bootstrap_server: consumerBootstrapServer,
                   consumer_kafka_topic: consumerKafkaTopic,
                   consumer_offset: consumerOffset,
                   consumer_timeout: consumerTimeout
                  },
            type: 'POST',
            success: function(response) {
                var responseString = JSON.stringify(response)
                $("#consumer_messages").val($("#consumer_messages").val() + responseString + "\n");
            },
            error: function(error) {
                $("#consumer_messages").val(error);
            }
        });
    });



});