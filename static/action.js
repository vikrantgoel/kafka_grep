$(document).ready(function() {

    var clipboard = new Clipboard(document.getElementById('consumer_copy_to_clipboard'));
    clipboard.on('error', function(e) {
        console.error('Action:', e.action);
        console.error('Trigger:', e.trigger);
    });

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    $("#producer_reset").click(async function() {
        $("#producer_bootstrap_server").val('');
        $("#producer_kafka_topic").val('');
        $("#producer_message").val('');
        $("#producer_messages").val('');
    });

    $("#consumer_reset").click(async function() {
        $("#consumer_bootstrap_server").val('');
        $("#consumer_kafka_topic").val('');
        $("#consumer_group_id").val('');
        $("#consumer_offset").val('earliest');
        $("#consumer_messages").val('');
    });

    $("#producer_submit").click(async function() {

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

    $(document).on('click', '#consumer_submit', function() {
        var consumerBootstrapServer = $("#consumer_bootstrap_server").val() || "localhost:9092";
        var consumerKafkaTopic = $("#consumer_kafka_topic").val() || "smartpricing-aux-test";
        var consumerOffset = $("#consumer_offset").val();
        var consumerGroupId = $("#consumer_group_id").val() || new Date().getTime();
        var consumerResponseEvent = 'consumer_response' + new Date().getTime();

        var url = "http://" + document.domain + ":" + location.port;
        var socket = new io.connect(url + "/consumerSocket");

        socket.emit("consumer_request", {consumer_bootstrap_server: consumerBootstrapServer,
                                         consumer_kafka_topic: consumerKafkaTopic, consumer_offset: consumerOffset,
                                         consumer_group_id: consumerGroupId,
                                         consumer_response_event: consumerResponseEvent});

        $("#consumer_submit").html('Stop');
        $("#consumer_submit").removeClass('btn-primary');
        $("#consumer_submit").attr('id', 'consuming_submit');

        socket.on(consumerResponseEvent, function(response) {
            if(JSON.stringify(response.consumer_response) !== "null") {
                for (index in response.consumer_response.kafka_output) {
                   $("#consumer_messages").val($("#consumer_messages").val() + JSON.parse(JSON.stringify(response.consumer_response.kafka_output[index])) + "\n");
                }
            }
        });
    });

    $(document).on('click', '#consuming_submit', async function() {
        var url = "http://" + document.domain + ":" + location.port;
        var socket = new io.connect(url + "/consumerSocket");
        socket.emit("disconnect_request");

        $("#consuming_submit").html('Stopping...');
        $("#consuming_submit").attr('id', 'stopping_submit');
        await sleep(3000);

        $("#stopping_submit").html('Consume');
        $("#stopping_submit").addClass('btn-primary');
        $("#stopping_submit").attr('id', 'consumer_submit');
    });

});