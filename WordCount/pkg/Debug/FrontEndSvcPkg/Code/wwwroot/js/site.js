// Please see documentation at https://docs.microsoft.com/aspnet/core/client-side/bundling-and-minification
// for details on configuring this project to bundle and minify static web assets.

// Write your JavaScript code.

var app = angular.module('WordCountApp', ['ui.bootstrap']);
app.run(function () { });

app.controller('WordCountController', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {

    $scope.process = function (sourceText) {
        console.log("Called shit");
        $http.post('api/WordCount', "\"" + sourceText + "\"")
            .then(function (data, status) {
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

                for (let word in data["data"]) {
                    myConfig["graphset"][0]["options"]["words"].push({ "text": word, "count": data["data"][word] });
                }

                zingchart.render({
                    id: 'chart-container',
                    data: myConfig,
                    height: '100%',
                    width: '100%'
                });
            }, function (data, status) {
                console.log("No data received");
            });
    };

}]);