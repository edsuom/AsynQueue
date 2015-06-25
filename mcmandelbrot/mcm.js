// mcmandelbrot
//
// An example package for AsynQueue:
// Asynchronous task queueing based on the Twisted framework, with task
// prioritization and a powerful worker interface.
//
// Copyright (C) 2015 by Edwin A. Suominen,
// http://edsuom.com/AsynQueue
//
// See edsuom.com for API documentation as well as information about
// Ed's background and other projects, software and otherwise.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS
// IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language
// governing permissions and limitations under the License.

function buildQS(parameters) {
    // Adapted from Michael's answer at
    // http://stackoverflow.com/questions/316781/
    //   how-to-build-query-string-with-javascript
    var qs = "";
    for(var key in parameters) {
	var value = parameters[key];
	qs += encodeURIComponent(key) + "=" + encodeURIComponent(value) + "&";
    }
    if (qs.length > 0){
	qs = qs.substring(0, qs.length-1); //chop off last "&"
	qs = "?" + qs;
    }
    return qs;
}
function getParams() {
    var value;
    var params = {'cr':"", 'ci':"", 'crpm':""};
    for (var name in params) {
	value = document.getElementById(name).value;
	value = Math.min(value, +3);
	value = Math.max(value, -3);
	params[name] = value;
    }
    return params
}
function setParams(params) {
    for (var name in params) {
	params[name] = Math.min(params[name], +3);
	params[name] = Math.max(params[name], -3);
	document.getElementById(name).value = params[name];
    }
}
function updateImage(params) {
    if (params === undefined) {
	var params = getParams();
    } else {
	setParams(params);
    }
    params.N = document.getElementById('image').clientWidth;
    var qs = buildQS(params);
    document.getElementById('mandelbrot').src = "/image.png" + qs;
    var plink = "http://" + window.location.host + qs;
    document.getElementById('permalink').href = plink;
    params.N = 2048;
    var qs = buildQS(params);
    document.getElementById('hd').href = "/image.png" + qs;
}
function xy(event) {
    var p = {};
    var params = getParams();
    element = document.getElementById('mandelbrot');
    var x0 = 0; var y0 = 0;
    var Nx = element.clientWidth;
    var Ny = element.clientHeight;
    do {
	x0 += element.offsetLeft;
	y0 += element.offsetTop;
	element = element.offsetParent;
    } while (element != null);
    var x = (event.clientX - x0) / Nx;
    var y = (event.clientY - y0) / Ny;
    p.crpm = Number(params.crpm);
    p.cr = p.crpm * (2*x - 1) + Number(params.cr);
    p.ci = p.crpm * (1 - 2*y) + Number(params.ci);
    return p
}
function zoomIn(event) {
    var params = xy(event);
    params.crpm = 0.2 * params.crpm;
    updateImage(params);
    hover(event);
}
function zoomOut() {
    var params = getParams();
    params.crpm = 5 * params.crpm;
    updateImage(params);
}
function hover(event) {
    var params = xy(event)
    var scale = 1 / params.crpm;
    if (scale > 1E12) {
	scale = 0.1*Math.round(scale/1E11)
	var unit = "&nbsp;trillion:1"
    } else if (scale > 1E9) {
	scale = 0.1*Math.round(scale/1E8)
	var unit = "&nbsp;billion:1"
    } else if (scale > 1E6) {
	scale = 0.1*Math.round(scale/1E5)
	var unit = "M:1"
    } else if (scale > 1E3) {
	scale = 0.1*Math.round(scale/1E2)
	var unit = "K:1"

    } else {
	scale = Math.round(scale)
	var unit = ":1"
    }
    if (scale % 1) {
	scale = scale.toFixed(1)
    } else {
	scale = scale.toFixed(0)
    }
    var message = "Scale:&nbsp;" + scale + unit + " &emsp;"
                + "Center:&nbsp;(" + params.cr + ",&nbsp;" + params.ci + ")";
    document.getElementById('hover').innerHTML = message;
}
