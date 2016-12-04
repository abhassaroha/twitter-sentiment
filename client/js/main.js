var ws = new WebSocket("ws://localhost:8082");
ws.onopen = function(event) {
	console.log("Received onopen event", event);
}

ws.onmessage = function(event) {
	console.log("Received onmessage event", event);
}


angular.module('twitter-app', ['ngMaterial'])
.controller('AppController',function($scope){
});
angular.element(document).ready(function() {
	angular.bootstrap(document, ['twitter-app']);
});