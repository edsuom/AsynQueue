function buildUrl(url, parameters) {
    // Copied from Michael's answer at
    // http://stackoverflow.com/questions/316781/
    //   how-to-build-query-string-with-javascript
    var qs = "";
    for(var key in parameters) {
	var value = parameters[key];
	qs += encodeURIComponent(key) + "=" + encodeURIComponent(value) + "&";
    }
    if (qs.length > 0){
	qs = qs.substring(0, qs.length-1); //chop off last "&"
	url = url + "?" + qs;
    }
    return url;
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
    var img = document.getElementById('mandelbrot');
    params.N = document.getElementById('image').clientWidth;
    img.src = buildUrl("/image.png", params);
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
}
function zoomOut() {
    var params = getParams();
    params.crpm = 5 * params.crpm;
    updateImage(params);
}
function hover(event) {
    var params = xy(event)
    var message = "(" + params.cr + ", " + params.ci + "), +/-" + params.crpm;
    document.getElementById('hover').innerHTML = message;
}
