// Please see documentation at https://docs.microsoft.com/aspnet/core/client-side/bundling-and-minification
// for details on configuring this project to bundle and minify static web assets.

// Write your JavaScript code.

var app = angular.module('WordCountApp', ['ui.bootstrap']);
app.run(function () { });

var time0, time1;

app.controller('WordCountController', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    $scope.process = function (sourceText) {
        time0 = performance.now();
        $http.post('api/WordCount', "\"" + sourceText + "\"")
            .then(function (data, status) {
                                
            }, function (data, status) {
                console.log("No data received");
            });
    };

}]);

const connection = new signalR.HubConnectionBuilder().withUrl('/resulthub').build();
connection.on('jobComplete', displayData)

connection.onclose(() => {
    setTimeout(() => startConnection(connection), 2000)
})
startConnection(connection)


function startConnection(connection) {
    connection.start()
        .then(() => {
            console.log('connected')
        })
        .catch(() => {
            setTimeout(() => startConnection(connection), 2000)
        })
}

function displayData(data) {
    time1 = performance.now();
    console.log("Call took: " + (time1 - time0) + " milliseconds");
    console.log(data);
    ZC.LICENSE = ["569d52cefae586f634c54f86dc99e6a9", "b55b025e438fa8a98e32482b5f768ff5"];
    zingchart.MODULESDIR = "https://cdn.zingchart.com/modules/";
    var myConfig = {
        "graphset": [
            {
                "type": "wordcloud",
                "options": {
                    "style": {
                        "tooltip": {
                            visible: true,
                            text: '%text: %hits'
                        }
                    },
                    "words": []
                }
            }
        ]
    };

    const words = myConfig["graphset"][0]["options"]["words"];
    for (var word in data) {
        words.push({ "text": word, "count": data[word] });
    }

    zingchart.render({
        id: 'chart-container',
        data: myConfig,
        height: '100%',
        width: '100%'
    });
}