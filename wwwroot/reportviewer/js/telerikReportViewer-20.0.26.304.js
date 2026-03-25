/*
* TelerikReporting v20.0.26.304 (https://www.telerik.com/products/reporting.aspx)
* Copyright 2026 Progress Software EAD. All rights reserved.
*
* Telerik Reporting commercial licenses may be obtained at
* https://www.telerik.com/purchase/license-agreement/reporting.aspx
* If you do not own a commercial license, this file shall be governed by the trial license terms.
*/
var telerikReportViewer = (function (exports) {
	'use strict';

	var commonjsGlobal = typeof globalThis !== 'undefined' ? globalThis : typeof window !== 'undefined' ? window : typeof global !== 'undefined' ? global : typeof self !== 'undefined' ? self : {};

	var dist = {exports: {}};

	/*! @license DOMPurify 3.2.6 | (c) Cure53 and other contributors | Released under the Apache license 2.0 and Mozilla Public License 2.0 | github.com/cure53/DOMPurify/blob/3.2.6/LICENSE */

	var purify_cjs;
	var hasRequiredPurify_cjs;

	function requirePurify_cjs () {
		if (hasRequiredPurify_cjs) return purify_cjs;
		hasRequiredPurify_cjs = 1;

		const {
		  entries,
		  setPrototypeOf,
		  isFrozen,
		  getPrototypeOf,
		  getOwnPropertyDescriptor
		} = Object;
		let {
		  freeze,
		  seal,
		  create
		} = Object; // eslint-disable-line import/no-mutable-exports
		let {
		  apply,
		  construct
		} = typeof Reflect !== 'undefined' && Reflect;
		if (!freeze) {
		  freeze = function freeze(x) {
		    return x;
		  };
		}
		if (!seal) {
		  seal = function seal(x) {
		    return x;
		  };
		}
		if (!apply) {
		  apply = function apply(fun, thisValue, args) {
		    return fun.apply(thisValue, args);
		  };
		}
		if (!construct) {
		  construct = function construct(Func, args) {
		    return new Func(...args);
		  };
		}
		const arrayForEach = unapply(Array.prototype.forEach);
		const arrayLastIndexOf = unapply(Array.prototype.lastIndexOf);
		const arrayPop = unapply(Array.prototype.pop);
		const arrayPush = unapply(Array.prototype.push);
		const arraySplice = unapply(Array.prototype.splice);
		const stringToLowerCase = unapply(String.prototype.toLowerCase);
		const stringToString = unapply(String.prototype.toString);
		const stringMatch = unapply(String.prototype.match);
		const stringReplace = unapply(String.prototype.replace);
		const stringIndexOf = unapply(String.prototype.indexOf);
		const stringTrim = unapply(String.prototype.trim);
		const objectHasOwnProperty = unapply(Object.prototype.hasOwnProperty);
		const regExpTest = unapply(RegExp.prototype.test);
		const typeErrorCreate = unconstruct(TypeError);
		/**
		 * Creates a new function that calls the given function with a specified thisArg and arguments.
		 *
		 * @param func - The function to be wrapped and called.
		 * @returns A new function that calls the given function with a specified thisArg and arguments.
		 */
		function unapply(func) {
		  return function (thisArg) {
		    if (thisArg instanceof RegExp) {
		      thisArg.lastIndex = 0;
		    }
		    for (var _len = arguments.length, args = new Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
		      args[_key - 1] = arguments[_key];
		    }
		    return apply(func, thisArg, args);
		  };
		}
		/**
		 * Creates a new function that constructs an instance of the given constructor function with the provided arguments.
		 *
		 * @param func - The constructor function to be wrapped and called.
		 * @returns A new function that constructs an instance of the given constructor function with the provided arguments.
		 */
		function unconstruct(func) {
		  return function () {
		    for (var _len2 = arguments.length, args = new Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
		      args[_key2] = arguments[_key2];
		    }
		    return construct(func, args);
		  };
		}
		/**
		 * Add properties to a lookup table
		 *
		 * @param set - The set to which elements will be added.
		 * @param array - The array containing elements to be added to the set.
		 * @param transformCaseFunc - An optional function to transform the case of each element before adding to the set.
		 * @returns The modified set with added elements.
		 */
		function addToSet(set, array) {
		  let transformCaseFunc = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : stringToLowerCase;
		  if (setPrototypeOf) {
		    // Make 'in' and truthy checks like Boolean(set.constructor)
		    // independent of any properties defined on Object.prototype.
		    // Prevent prototype setters from intercepting set as a this value.
		    setPrototypeOf(set, null);
		  }
		  let l = array.length;
		  while (l--) {
		    let element = array[l];
		    if (typeof element === 'string') {
		      const lcElement = transformCaseFunc(element);
		      if (lcElement !== element) {
		        // Config presets (e.g. tags.js, attrs.js) are immutable.
		        if (!isFrozen(array)) {
		          array[l] = lcElement;
		        }
		        element = lcElement;
		      }
		    }
		    set[element] = true;
		  }
		  return set;
		}
		/**
		 * Clean up an array to harden against CSPP
		 *
		 * @param array - The array to be cleaned.
		 * @returns The cleaned version of the array
		 */
		function cleanArray(array) {
		  for (let index = 0; index < array.length; index++) {
		    const isPropertyExist = objectHasOwnProperty(array, index);
		    if (!isPropertyExist) {
		      array[index] = null;
		    }
		  }
		  return array;
		}
		/**
		 * Shallow clone an object
		 *
		 * @param object - The object to be cloned.
		 * @returns A new object that copies the original.
		 */
		function clone(object) {
		  const newObject = create(null);
		  for (const [property, value] of entries(object)) {
		    const isPropertyExist = objectHasOwnProperty(object, property);
		    if (isPropertyExist) {
		      if (Array.isArray(value)) {
		        newObject[property] = cleanArray(value);
		      } else if (value && typeof value === 'object' && value.constructor === Object) {
		        newObject[property] = clone(value);
		      } else {
		        newObject[property] = value;
		      }
		    }
		  }
		  return newObject;
		}
		/**
		 * This method automatically checks if the prop is function or getter and behaves accordingly.
		 *
		 * @param object - The object to look up the getter function in its prototype chain.
		 * @param prop - The property name for which to find the getter function.
		 * @returns The getter function found in the prototype chain or a fallback function.
		 */
		function lookupGetter(object, prop) {
		  while (object !== null) {
		    const desc = getOwnPropertyDescriptor(object, prop);
		    if (desc) {
		      if (desc.get) {
		        return unapply(desc.get);
		      }
		      if (typeof desc.value === 'function') {
		        return unapply(desc.value);
		      }
		    }
		    object = getPrototypeOf(object);
		  }
		  function fallbackValue() {
		    return null;
		  }
		  return fallbackValue;
		}

		const html$1 = freeze(['a', 'abbr', 'acronym', 'address', 'area', 'article', 'aside', 'audio', 'b', 'bdi', 'bdo', 'big', 'blink', 'blockquote', 'body', 'br', 'button', 'canvas', 'caption', 'center', 'cite', 'code', 'col', 'colgroup', 'content', 'data', 'datalist', 'dd', 'decorator', 'del', 'details', 'dfn', 'dialog', 'dir', 'div', 'dl', 'dt', 'element', 'em', 'fieldset', 'figcaption', 'figure', 'font', 'footer', 'form', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'head', 'header', 'hgroup', 'hr', 'html', 'i', 'img', 'input', 'ins', 'kbd', 'label', 'legend', 'li', 'main', 'map', 'mark', 'marquee', 'menu', 'menuitem', 'meter', 'nav', 'nobr', 'ol', 'optgroup', 'option', 'output', 'p', 'picture', 'pre', 'progress', 'q', 'rp', 'rt', 'ruby', 's', 'samp', 'section', 'select', 'shadow', 'small', 'source', 'spacer', 'span', 'strike', 'strong', 'style', 'sub', 'summary', 'sup', 'table', 'tbody', 'td', 'template', 'textarea', 'tfoot', 'th', 'thead', 'time', 'tr', 'track', 'tt', 'u', 'ul', 'var', 'video', 'wbr']);
		const svg$1 = freeze(['svg', 'a', 'altglyph', 'altglyphdef', 'altglyphitem', 'animatecolor', 'animatemotion', 'animatetransform', 'circle', 'clippath', 'defs', 'desc', 'ellipse', 'filter', 'font', 'g', 'glyph', 'glyphref', 'hkern', 'image', 'line', 'lineargradient', 'marker', 'mask', 'metadata', 'mpath', 'path', 'pattern', 'polygon', 'polyline', 'radialgradient', 'rect', 'stop', 'style', 'switch', 'symbol', 'text', 'textpath', 'title', 'tref', 'tspan', 'view', 'vkern']);
		const svgFilters = freeze(['feBlend', 'feColorMatrix', 'feComponentTransfer', 'feComposite', 'feConvolveMatrix', 'feDiffuseLighting', 'feDisplacementMap', 'feDistantLight', 'feDropShadow', 'feFlood', 'feFuncA', 'feFuncB', 'feFuncG', 'feFuncR', 'feGaussianBlur', 'feImage', 'feMerge', 'feMergeNode', 'feMorphology', 'feOffset', 'fePointLight', 'feSpecularLighting', 'feSpotLight', 'feTile', 'feTurbulence']);
		// List of SVG elements that are disallowed by default.
		// We still need to know them so that we can do namespace
		// checks properly in case one wants to add them to
		// allow-list.
		const svgDisallowed = freeze(['animate', 'color-profile', 'cursor', 'discard', 'font-face', 'font-face-format', 'font-face-name', 'font-face-src', 'font-face-uri', 'foreignobject', 'hatch', 'hatchpath', 'mesh', 'meshgradient', 'meshpatch', 'meshrow', 'missing-glyph', 'script', 'set', 'solidcolor', 'unknown', 'use']);
		const mathMl$1 = freeze(['math', 'menclose', 'merror', 'mfenced', 'mfrac', 'mglyph', 'mi', 'mlabeledtr', 'mmultiscripts', 'mn', 'mo', 'mover', 'mpadded', 'mphantom', 'mroot', 'mrow', 'ms', 'mspace', 'msqrt', 'mstyle', 'msub', 'msup', 'msubsup', 'mtable', 'mtd', 'mtext', 'mtr', 'munder', 'munderover', 'mprescripts']);
		// Similarly to SVG, we want to know all MathML elements,
		// even those that we disallow by default.
		const mathMlDisallowed = freeze(['maction', 'maligngroup', 'malignmark', 'mlongdiv', 'mscarries', 'mscarry', 'msgroup', 'mstack', 'msline', 'msrow', 'semantics', 'annotation', 'annotation-xml', 'mprescripts', 'none']);
		const text = freeze(['#text']);

		const html = freeze(['accept', 'action', 'align', 'alt', 'autocapitalize', 'autocomplete', 'autopictureinpicture', 'autoplay', 'background', 'bgcolor', 'border', 'capture', 'cellpadding', 'cellspacing', 'checked', 'cite', 'class', 'clear', 'color', 'cols', 'colspan', 'controls', 'controlslist', 'coords', 'crossorigin', 'datetime', 'decoding', 'default', 'dir', 'disabled', 'disablepictureinpicture', 'disableremoteplayback', 'download', 'draggable', 'enctype', 'enterkeyhint', 'face', 'for', 'headers', 'height', 'hidden', 'high', 'href', 'hreflang', 'id', 'inputmode', 'integrity', 'ismap', 'kind', 'label', 'lang', 'list', 'loading', 'loop', 'low', 'max', 'maxlength', 'media', 'method', 'min', 'minlength', 'multiple', 'muted', 'name', 'nonce', 'noshade', 'novalidate', 'nowrap', 'open', 'optimum', 'pattern', 'placeholder', 'playsinline', 'popover', 'popovertarget', 'popovertargetaction', 'poster', 'preload', 'pubdate', 'radiogroup', 'readonly', 'rel', 'required', 'rev', 'reversed', 'role', 'rows', 'rowspan', 'spellcheck', 'scope', 'selected', 'shape', 'size', 'sizes', 'span', 'srclang', 'start', 'src', 'srcset', 'step', 'style', 'summary', 'tabindex', 'title', 'translate', 'type', 'usemap', 'valign', 'value', 'width', 'wrap', 'xmlns', 'slot']);
		const svg = freeze(['accent-height', 'accumulate', 'additive', 'alignment-baseline', 'amplitude', 'ascent', 'attributename', 'attributetype', 'azimuth', 'basefrequency', 'baseline-shift', 'begin', 'bias', 'by', 'class', 'clip', 'clippathunits', 'clip-path', 'clip-rule', 'color', 'color-interpolation', 'color-interpolation-filters', 'color-profile', 'color-rendering', 'cx', 'cy', 'd', 'dx', 'dy', 'diffuseconstant', 'direction', 'display', 'divisor', 'dur', 'edgemode', 'elevation', 'end', 'exponent', 'fill', 'fill-opacity', 'fill-rule', 'filter', 'filterunits', 'flood-color', 'flood-opacity', 'font-family', 'font-size', 'font-size-adjust', 'font-stretch', 'font-style', 'font-variant', 'font-weight', 'fx', 'fy', 'g1', 'g2', 'glyph-name', 'glyphref', 'gradientunits', 'gradienttransform', 'height', 'href', 'id', 'image-rendering', 'in', 'in2', 'intercept', 'k', 'k1', 'k2', 'k3', 'k4', 'kerning', 'keypoints', 'keysplines', 'keytimes', 'lang', 'lengthadjust', 'letter-spacing', 'kernelmatrix', 'kernelunitlength', 'lighting-color', 'local', 'marker-end', 'marker-mid', 'marker-start', 'markerheight', 'markerunits', 'markerwidth', 'maskcontentunits', 'maskunits', 'max', 'mask', 'media', 'method', 'mode', 'min', 'name', 'numoctaves', 'offset', 'operator', 'opacity', 'order', 'orient', 'orientation', 'origin', 'overflow', 'paint-order', 'path', 'pathlength', 'patterncontentunits', 'patterntransform', 'patternunits', 'points', 'preservealpha', 'preserveaspectratio', 'primitiveunits', 'r', 'rx', 'ry', 'radius', 'refx', 'refy', 'repeatcount', 'repeatdur', 'restart', 'result', 'rotate', 'scale', 'seed', 'shape-rendering', 'slope', 'specularconstant', 'specularexponent', 'spreadmethod', 'startoffset', 'stddeviation', 'stitchtiles', 'stop-color', 'stop-opacity', 'stroke-dasharray', 'stroke-dashoffset', 'stroke-linecap', 'stroke-linejoin', 'stroke-miterlimit', 'stroke-opacity', 'stroke', 'stroke-width', 'style', 'surfacescale', 'systemlanguage', 'tabindex', 'tablevalues', 'targetx', 'targety', 'transform', 'transform-origin', 'text-anchor', 'text-decoration', 'text-rendering', 'textlength', 'type', 'u1', 'u2', 'unicode', 'values', 'viewbox', 'visibility', 'version', 'vert-adv-y', 'vert-origin-x', 'vert-origin-y', 'width', 'word-spacing', 'wrap', 'writing-mode', 'xchannelselector', 'ychannelselector', 'x', 'x1', 'x2', 'xmlns', 'y', 'y1', 'y2', 'z', 'zoomandpan']);
		const mathMl = freeze(['accent', 'accentunder', 'align', 'bevelled', 'close', 'columnsalign', 'columnlines', 'columnspan', 'denomalign', 'depth', 'dir', 'display', 'displaystyle', 'encoding', 'fence', 'frame', 'height', 'href', 'id', 'largeop', 'length', 'linethickness', 'lspace', 'lquote', 'mathbackground', 'mathcolor', 'mathsize', 'mathvariant', 'maxsize', 'minsize', 'movablelimits', 'notation', 'numalign', 'open', 'rowalign', 'rowlines', 'rowspacing', 'rowspan', 'rspace', 'rquote', 'scriptlevel', 'scriptminsize', 'scriptsizemultiplier', 'selection', 'separator', 'separators', 'stretchy', 'subscriptshift', 'supscriptshift', 'symmetric', 'voffset', 'width', 'xmlns']);
		const xml = freeze(['xlink:href', 'xml:id', 'xlink:title', 'xml:space', 'xmlns:xlink']);

		// eslint-disable-next-line unicorn/better-regex
		const MUSTACHE_EXPR = seal(/\{\{[\w\W]*|[\w\W]*\}\}/gm); // Specify template detection regex for SAFE_FOR_TEMPLATES mode
		const ERB_EXPR = seal(/<%[\w\W]*|[\w\W]*%>/gm);
		const TMPLIT_EXPR = seal(/\$\{[\w\W]*/gm); // eslint-disable-line unicorn/better-regex
		const DATA_ATTR = seal(/^data-[\-\w.\u00B7-\uFFFF]+$/); // eslint-disable-line no-useless-escape
		const ARIA_ATTR = seal(/^aria-[\-\w]+$/); // eslint-disable-line no-useless-escape
		const IS_ALLOWED_URI = seal(/^(?:(?:(?:f|ht)tps?|mailto|tel|callto|sms|cid|xmpp|matrix):|[^a-z]|[a-z+.\-]+(?:[^a-z+.\-:]|$))/i // eslint-disable-line no-useless-escape
		);
		const IS_SCRIPT_OR_DATA = seal(/^(?:\w+script|data):/i);
		const ATTR_WHITESPACE = seal(/[\u0000-\u0020\u00A0\u1680\u180E\u2000-\u2029\u205F\u3000]/g // eslint-disable-line no-control-regex
		);
		const DOCTYPE_NAME = seal(/^html$/i);
		const CUSTOM_ELEMENT = seal(/^[a-z][.\w]*(-[.\w]+)+$/i);

		var EXPRESSIONS = /*#__PURE__*/Object.freeze({
		  __proto__: null,
		  ARIA_ATTR: ARIA_ATTR,
		  ATTR_WHITESPACE: ATTR_WHITESPACE,
		  CUSTOM_ELEMENT: CUSTOM_ELEMENT,
		  DATA_ATTR: DATA_ATTR,
		  DOCTYPE_NAME: DOCTYPE_NAME,
		  ERB_EXPR: ERB_EXPR,
		  IS_ALLOWED_URI: IS_ALLOWED_URI,
		  IS_SCRIPT_OR_DATA: IS_SCRIPT_OR_DATA,
		  MUSTACHE_EXPR: MUSTACHE_EXPR,
		  TMPLIT_EXPR: TMPLIT_EXPR
		});

		/* eslint-disable @typescript-eslint/indent */
		// https://developer.mozilla.org/en-US/docs/Web/API/Node/nodeType
		const NODE_TYPE = {
		  element: 1,
		  text: 3,
		  // Deprecated
		  progressingInstruction: 7,
		  comment: 8,
		  document: 9};
		const getGlobal = function getGlobal() {
		  return typeof window === 'undefined' ? null : window;
		};
		/**
		 * Creates a no-op policy for internal use only.
		 * Don't export this function outside this module!
		 * @param trustedTypes The policy factory.
		 * @param purifyHostElement The Script element used to load DOMPurify (to determine policy name suffix).
		 * @return The policy created (or null, if Trusted Types
		 * are not supported or creating the policy failed).
		 */
		const _createTrustedTypesPolicy = function _createTrustedTypesPolicy(trustedTypes, purifyHostElement) {
		  if (typeof trustedTypes !== 'object' || typeof trustedTypes.createPolicy !== 'function') {
		    return null;
		  }
		  // Allow the callers to control the unique policy name
		  // by adding a data-tt-policy-suffix to the script element with the DOMPurify.
		  // Policy creation with duplicate names throws in Trusted Types.
		  let suffix = null;
		  const ATTR_NAME = 'data-tt-policy-suffix';
		  if (purifyHostElement && purifyHostElement.hasAttribute(ATTR_NAME)) {
		    suffix = purifyHostElement.getAttribute(ATTR_NAME);
		  }
		  const policyName = 'dompurify' + (suffix ? '#' + suffix : '');
		  try {
		    return trustedTypes.createPolicy(policyName, {
		      createHTML(html) {
		        return html;
		      },
		      createScriptURL(scriptUrl) {
		        return scriptUrl;
		      }
		    });
		  } catch (_) {
		    // Policy creation failed (most likely another DOMPurify script has
		    // already run). Skip creating the policy, as this will only cause errors
		    // if TT are enforced.
		    console.warn('TrustedTypes policy ' + policyName + ' could not be created.');
		    return null;
		  }
		};
		const _createHooksMap = function _createHooksMap() {
		  return {
		    afterSanitizeAttributes: [],
		    afterSanitizeElements: [],
		    afterSanitizeShadowDOM: [],
		    beforeSanitizeAttributes: [],
		    beforeSanitizeElements: [],
		    beforeSanitizeShadowDOM: [],
		    uponSanitizeAttribute: [],
		    uponSanitizeElement: [],
		    uponSanitizeShadowNode: []
		  };
		};
		function createDOMPurify() {
		  let window = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : getGlobal();
		  const DOMPurify = root => createDOMPurify(root);
		  DOMPurify.version = '3.2.6';
		  DOMPurify.removed = [];
		  if (!window || !window.document || window.document.nodeType !== NODE_TYPE.document || !window.Element) {
		    // Not running in a browser, provide a factory function
		    // so that you can pass your own Window
		    DOMPurify.isSupported = false;
		    return DOMPurify;
		  }
		  let {
		    document
		  } = window;
		  const originalDocument = document;
		  const currentScript = originalDocument.currentScript;
		  const {
		    DocumentFragment,
		    HTMLTemplateElement,
		    Node,
		    Element,
		    NodeFilter,
		    NamedNodeMap = window.NamedNodeMap || window.MozNamedAttrMap,
		    HTMLFormElement,
		    DOMParser,
		    trustedTypes
		  } = window;
		  const ElementPrototype = Element.prototype;
		  const cloneNode = lookupGetter(ElementPrototype, 'cloneNode');
		  const remove = lookupGetter(ElementPrototype, 'remove');
		  const getNextSibling = lookupGetter(ElementPrototype, 'nextSibling');
		  const getChildNodes = lookupGetter(ElementPrototype, 'childNodes');
		  const getParentNode = lookupGetter(ElementPrototype, 'parentNode');
		  // As per issue #47, the web-components registry is inherited by a
		  // new document created via createHTMLDocument. As per the spec
		  // (http://w3c.github.io/webcomponents/spec/custom/#creating-and-passing-registries)
		  // a new empty registry is used when creating a template contents owner
		  // document, so we use that as our parent document to ensure nothing
		  // is inherited.
		  if (typeof HTMLTemplateElement === 'function') {
		    const template = document.createElement('template');
		    if (template.content && template.content.ownerDocument) {
		      document = template.content.ownerDocument;
		    }
		  }
		  let trustedTypesPolicy;
		  let emptyHTML = '';
		  const {
		    implementation,
		    createNodeIterator,
		    createDocumentFragment,
		    getElementsByTagName
		  } = document;
		  const {
		    importNode
		  } = originalDocument;
		  let hooks = _createHooksMap();
		  /**
		   * Expose whether this browser supports running the full DOMPurify.
		   */
		  DOMPurify.isSupported = typeof entries === 'function' && typeof getParentNode === 'function' && implementation && implementation.createHTMLDocument !== undefined;
		  const {
		    MUSTACHE_EXPR,
		    ERB_EXPR,
		    TMPLIT_EXPR,
		    DATA_ATTR,
		    ARIA_ATTR,
		    IS_SCRIPT_OR_DATA,
		    ATTR_WHITESPACE,
		    CUSTOM_ELEMENT
		  } = EXPRESSIONS;
		  let {
		    IS_ALLOWED_URI: IS_ALLOWED_URI$1
		  } = EXPRESSIONS;
		  /**
		   * We consider the elements and attributes below to be safe. Ideally
		   * don't add any new ones but feel free to remove unwanted ones.
		   */
		  /* allowed element names */
		  let ALLOWED_TAGS = null;
		  const DEFAULT_ALLOWED_TAGS = addToSet({}, [...html$1, ...svg$1, ...svgFilters, ...mathMl$1, ...text]);
		  /* Allowed attribute names */
		  let ALLOWED_ATTR = null;
		  const DEFAULT_ALLOWED_ATTR = addToSet({}, [...html, ...svg, ...mathMl, ...xml]);
		  /*
		   * Configure how DOMPurify should handle custom elements and their attributes as well as customized built-in elements.
		   * @property {RegExp|Function|null} tagNameCheck one of [null, regexPattern, predicate]. Default: `null` (disallow any custom elements)
		   * @property {RegExp|Function|null} attributeNameCheck one of [null, regexPattern, predicate]. Default: `null` (disallow any attributes not on the allow list)
		   * @property {boolean} allowCustomizedBuiltInElements allow custom elements derived from built-ins if they pass CUSTOM_ELEMENT_HANDLING.tagNameCheck. Default: `false`.
		   */
		  let CUSTOM_ELEMENT_HANDLING = Object.seal(create(null, {
		    tagNameCheck: {
		      writable: true,
		      configurable: false,
		      enumerable: true,
		      value: null
		    },
		    attributeNameCheck: {
		      writable: true,
		      configurable: false,
		      enumerable: true,
		      value: null
		    },
		    allowCustomizedBuiltInElements: {
		      writable: true,
		      configurable: false,
		      enumerable: true,
		      value: false
		    }
		  }));
		  /* Explicitly forbidden tags (overrides ALLOWED_TAGS/ADD_TAGS) */
		  let FORBID_TAGS = null;
		  /* Explicitly forbidden attributes (overrides ALLOWED_ATTR/ADD_ATTR) */
		  let FORBID_ATTR = null;
		  /* Decide if ARIA attributes are okay */
		  let ALLOW_ARIA_ATTR = true;
		  /* Decide if custom data attributes are okay */
		  let ALLOW_DATA_ATTR = true;
		  /* Decide if unknown protocols are okay */
		  let ALLOW_UNKNOWN_PROTOCOLS = false;
		  /* Decide if self-closing tags in attributes are allowed.
		   * Usually removed due to a mXSS issue in jQuery 3.0 */
		  let ALLOW_SELF_CLOSE_IN_ATTR = true;
		  /* Output should be safe for common template engines.
		   * This means, DOMPurify removes data attributes, mustaches and ERB
		   */
		  let SAFE_FOR_TEMPLATES = false;
		  /* Output should be safe even for XML used within HTML and alike.
		   * This means, DOMPurify removes comments when containing risky content.
		   */
		  let SAFE_FOR_XML = true;
		  /* Decide if document with <html>... should be returned */
		  let WHOLE_DOCUMENT = false;
		  /* Track whether config is already set on this instance of DOMPurify. */
		  let SET_CONFIG = false;
		  /* Decide if all elements (e.g. style, script) must be children of
		   * document.body. By default, browsers might move them to document.head */
		  let FORCE_BODY = false;
		  /* Decide if a DOM `HTMLBodyElement` should be returned, instead of a html
		   * string (or a TrustedHTML object if Trusted Types are supported).
		   * If `WHOLE_DOCUMENT` is enabled a `HTMLHtmlElement` will be returned instead
		   */
		  let RETURN_DOM = false;
		  /* Decide if a DOM `DocumentFragment` should be returned, instead of a html
		   * string  (or a TrustedHTML object if Trusted Types are supported) */
		  let RETURN_DOM_FRAGMENT = false;
		  /* Try to return a Trusted Type object instead of a string, return a string in
		   * case Trusted Types are not supported  */
		  let RETURN_TRUSTED_TYPE = false;
		  /* Output should be free from DOM clobbering attacks?
		   * This sanitizes markups named with colliding, clobberable built-in DOM APIs.
		   */
		  let SANITIZE_DOM = true;
		  /* Achieve full DOM Clobbering protection by isolating the namespace of named
		   * properties and JS variables, mitigating attacks that abuse the HTML/DOM spec rules.
		   *
		   * HTML/DOM spec rules that enable DOM Clobbering:
		   *   - Named Access on Window (§7.3.3)
		   *   - DOM Tree Accessors (§3.1.5)
		   *   - Form Element Parent-Child Relations (§4.10.3)
		   *   - Iframe srcdoc / Nested WindowProxies (§4.8.5)
		   *   - HTMLCollection (§4.2.10.2)
		   *
		   * Namespace isolation is implemented by prefixing `id` and `name` attributes
		   * with a constant string, i.e., `user-content-`
		   */
		  let SANITIZE_NAMED_PROPS = false;
		  const SANITIZE_NAMED_PROPS_PREFIX = 'user-content-';
		  /* Keep element content when removing element? */
		  let KEEP_CONTENT = true;
		  /* If a `Node` is passed to sanitize(), then performs sanitization in-place instead
		   * of importing it into a new Document and returning a sanitized copy */
		  let IN_PLACE = false;
		  /* Allow usage of profiles like html, svg and mathMl */
		  let USE_PROFILES = {};
		  /* Tags to ignore content of when KEEP_CONTENT is true */
		  let FORBID_CONTENTS = null;
		  const DEFAULT_FORBID_CONTENTS = addToSet({}, ['annotation-xml', 'audio', 'colgroup', 'desc', 'foreignobject', 'head', 'iframe', 'math', 'mi', 'mn', 'mo', 'ms', 'mtext', 'noembed', 'noframes', 'noscript', 'plaintext', 'script', 'style', 'svg', 'template', 'thead', 'title', 'video', 'xmp']);
		  /* Tags that are safe for data: URIs */
		  let DATA_URI_TAGS = null;
		  const DEFAULT_DATA_URI_TAGS = addToSet({}, ['audio', 'video', 'img', 'source', 'image', 'track']);
		  /* Attributes safe for values like "javascript:" */
		  let URI_SAFE_ATTRIBUTES = null;
		  const DEFAULT_URI_SAFE_ATTRIBUTES = addToSet({}, ['alt', 'class', 'for', 'id', 'label', 'name', 'pattern', 'placeholder', 'role', 'summary', 'title', 'value', 'style', 'xmlns']);
		  const MATHML_NAMESPACE = 'http://www.w3.org/1998/Math/MathML';
		  const SVG_NAMESPACE = 'http://www.w3.org/2000/svg';
		  const HTML_NAMESPACE = 'http://www.w3.org/1999/xhtml';
		  /* Document namespace */
		  let NAMESPACE = HTML_NAMESPACE;
		  let IS_EMPTY_INPUT = false;
		  /* Allowed XHTML+XML namespaces */
		  let ALLOWED_NAMESPACES = null;
		  const DEFAULT_ALLOWED_NAMESPACES = addToSet({}, [MATHML_NAMESPACE, SVG_NAMESPACE, HTML_NAMESPACE], stringToString);
		  let MATHML_TEXT_INTEGRATION_POINTS = addToSet({}, ['mi', 'mo', 'mn', 'ms', 'mtext']);
		  let HTML_INTEGRATION_POINTS = addToSet({}, ['annotation-xml']);
		  // Certain elements are allowed in both SVG and HTML
		  // namespace. We need to specify them explicitly
		  // so that they don't get erroneously deleted from
		  // HTML namespace.
		  const COMMON_SVG_AND_HTML_ELEMENTS = addToSet({}, ['title', 'style', 'font', 'a', 'script']);
		  /* Parsing of strict XHTML documents */
		  let PARSER_MEDIA_TYPE = null;
		  const SUPPORTED_PARSER_MEDIA_TYPES = ['application/xhtml+xml', 'text/html'];
		  const DEFAULT_PARSER_MEDIA_TYPE = 'text/html';
		  let transformCaseFunc = null;
		  /* Keep a reference to config to pass to hooks */
		  let CONFIG = null;
		  /* Ideally, do not touch anything below this line */
		  /* ______________________________________________ */
		  const formElement = document.createElement('form');
		  const isRegexOrFunction = function isRegexOrFunction(testValue) {
		    return testValue instanceof RegExp || testValue instanceof Function;
		  };
		  /**
		   * _parseConfig
		   *
		   * @param cfg optional config literal
		   */
		  // eslint-disable-next-line complexity
		  const _parseConfig = function _parseConfig() {
		    let cfg = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
		    if (CONFIG && CONFIG === cfg) {
		      return;
		    }
		    /* Shield configuration object from tampering */
		    if (!cfg || typeof cfg !== 'object') {
		      cfg = {};
		    }
		    /* Shield configuration object from prototype pollution */
		    cfg = clone(cfg);
		    PARSER_MEDIA_TYPE =
		    // eslint-disable-next-line unicorn/prefer-includes
		    SUPPORTED_PARSER_MEDIA_TYPES.indexOf(cfg.PARSER_MEDIA_TYPE) === -1 ? DEFAULT_PARSER_MEDIA_TYPE : cfg.PARSER_MEDIA_TYPE;
		    // HTML tags and attributes are not case-sensitive, converting to lowercase. Keeping XHTML as is.
		    transformCaseFunc = PARSER_MEDIA_TYPE === 'application/xhtml+xml' ? stringToString : stringToLowerCase;
		    /* Set configuration parameters */
		    ALLOWED_TAGS = objectHasOwnProperty(cfg, 'ALLOWED_TAGS') ? addToSet({}, cfg.ALLOWED_TAGS, transformCaseFunc) : DEFAULT_ALLOWED_TAGS;
		    ALLOWED_ATTR = objectHasOwnProperty(cfg, 'ALLOWED_ATTR') ? addToSet({}, cfg.ALLOWED_ATTR, transformCaseFunc) : DEFAULT_ALLOWED_ATTR;
		    ALLOWED_NAMESPACES = objectHasOwnProperty(cfg, 'ALLOWED_NAMESPACES') ? addToSet({}, cfg.ALLOWED_NAMESPACES, stringToString) : DEFAULT_ALLOWED_NAMESPACES;
		    URI_SAFE_ATTRIBUTES = objectHasOwnProperty(cfg, 'ADD_URI_SAFE_ATTR') ? addToSet(clone(DEFAULT_URI_SAFE_ATTRIBUTES), cfg.ADD_URI_SAFE_ATTR, transformCaseFunc) : DEFAULT_URI_SAFE_ATTRIBUTES;
		    DATA_URI_TAGS = objectHasOwnProperty(cfg, 'ADD_DATA_URI_TAGS') ? addToSet(clone(DEFAULT_DATA_URI_TAGS), cfg.ADD_DATA_URI_TAGS, transformCaseFunc) : DEFAULT_DATA_URI_TAGS;
		    FORBID_CONTENTS = objectHasOwnProperty(cfg, 'FORBID_CONTENTS') ? addToSet({}, cfg.FORBID_CONTENTS, transformCaseFunc) : DEFAULT_FORBID_CONTENTS;
		    FORBID_TAGS = objectHasOwnProperty(cfg, 'FORBID_TAGS') ? addToSet({}, cfg.FORBID_TAGS, transformCaseFunc) : clone({});
		    FORBID_ATTR = objectHasOwnProperty(cfg, 'FORBID_ATTR') ? addToSet({}, cfg.FORBID_ATTR, transformCaseFunc) : clone({});
		    USE_PROFILES = objectHasOwnProperty(cfg, 'USE_PROFILES') ? cfg.USE_PROFILES : false;
		    ALLOW_ARIA_ATTR = cfg.ALLOW_ARIA_ATTR !== false; // Default true
		    ALLOW_DATA_ATTR = cfg.ALLOW_DATA_ATTR !== false; // Default true
		    ALLOW_UNKNOWN_PROTOCOLS = cfg.ALLOW_UNKNOWN_PROTOCOLS || false; // Default false
		    ALLOW_SELF_CLOSE_IN_ATTR = cfg.ALLOW_SELF_CLOSE_IN_ATTR !== false; // Default true
		    SAFE_FOR_TEMPLATES = cfg.SAFE_FOR_TEMPLATES || false; // Default false
		    SAFE_FOR_XML = cfg.SAFE_FOR_XML !== false; // Default true
		    WHOLE_DOCUMENT = cfg.WHOLE_DOCUMENT || false; // Default false
		    RETURN_DOM = cfg.RETURN_DOM || false; // Default false
		    RETURN_DOM_FRAGMENT = cfg.RETURN_DOM_FRAGMENT || false; // Default false
		    RETURN_TRUSTED_TYPE = cfg.RETURN_TRUSTED_TYPE || false; // Default false
		    FORCE_BODY = cfg.FORCE_BODY || false; // Default false
		    SANITIZE_DOM = cfg.SANITIZE_DOM !== false; // Default true
		    SANITIZE_NAMED_PROPS = cfg.SANITIZE_NAMED_PROPS || false; // Default false
		    KEEP_CONTENT = cfg.KEEP_CONTENT !== false; // Default true
		    IN_PLACE = cfg.IN_PLACE || false; // Default false
		    IS_ALLOWED_URI$1 = cfg.ALLOWED_URI_REGEXP || IS_ALLOWED_URI;
		    NAMESPACE = cfg.NAMESPACE || HTML_NAMESPACE;
		    MATHML_TEXT_INTEGRATION_POINTS = cfg.MATHML_TEXT_INTEGRATION_POINTS || MATHML_TEXT_INTEGRATION_POINTS;
		    HTML_INTEGRATION_POINTS = cfg.HTML_INTEGRATION_POINTS || HTML_INTEGRATION_POINTS;
		    CUSTOM_ELEMENT_HANDLING = cfg.CUSTOM_ELEMENT_HANDLING || {};
		    if (cfg.CUSTOM_ELEMENT_HANDLING && isRegexOrFunction(cfg.CUSTOM_ELEMENT_HANDLING.tagNameCheck)) {
		      CUSTOM_ELEMENT_HANDLING.tagNameCheck = cfg.CUSTOM_ELEMENT_HANDLING.tagNameCheck;
		    }
		    if (cfg.CUSTOM_ELEMENT_HANDLING && isRegexOrFunction(cfg.CUSTOM_ELEMENT_HANDLING.attributeNameCheck)) {
		      CUSTOM_ELEMENT_HANDLING.attributeNameCheck = cfg.CUSTOM_ELEMENT_HANDLING.attributeNameCheck;
		    }
		    if (cfg.CUSTOM_ELEMENT_HANDLING && typeof cfg.CUSTOM_ELEMENT_HANDLING.allowCustomizedBuiltInElements === 'boolean') {
		      CUSTOM_ELEMENT_HANDLING.allowCustomizedBuiltInElements = cfg.CUSTOM_ELEMENT_HANDLING.allowCustomizedBuiltInElements;
		    }
		    if (SAFE_FOR_TEMPLATES) {
		      ALLOW_DATA_ATTR = false;
		    }
		    if (RETURN_DOM_FRAGMENT) {
		      RETURN_DOM = true;
		    }
		    /* Parse profile info */
		    if (USE_PROFILES) {
		      ALLOWED_TAGS = addToSet({}, text);
		      ALLOWED_ATTR = [];
		      if (USE_PROFILES.html === true) {
		        addToSet(ALLOWED_TAGS, html$1);
		        addToSet(ALLOWED_ATTR, html);
		      }
		      if (USE_PROFILES.svg === true) {
		        addToSet(ALLOWED_TAGS, svg$1);
		        addToSet(ALLOWED_ATTR, svg);
		        addToSet(ALLOWED_ATTR, xml);
		      }
		      if (USE_PROFILES.svgFilters === true) {
		        addToSet(ALLOWED_TAGS, svgFilters);
		        addToSet(ALLOWED_ATTR, svg);
		        addToSet(ALLOWED_ATTR, xml);
		      }
		      if (USE_PROFILES.mathMl === true) {
		        addToSet(ALLOWED_TAGS, mathMl$1);
		        addToSet(ALLOWED_ATTR, mathMl);
		        addToSet(ALLOWED_ATTR, xml);
		      }
		    }
		    /* Merge configuration parameters */
		    if (cfg.ADD_TAGS) {
		      if (ALLOWED_TAGS === DEFAULT_ALLOWED_TAGS) {
		        ALLOWED_TAGS = clone(ALLOWED_TAGS);
		      }
		      addToSet(ALLOWED_TAGS, cfg.ADD_TAGS, transformCaseFunc);
		    }
		    if (cfg.ADD_ATTR) {
		      if (ALLOWED_ATTR === DEFAULT_ALLOWED_ATTR) {
		        ALLOWED_ATTR = clone(ALLOWED_ATTR);
		      }
		      addToSet(ALLOWED_ATTR, cfg.ADD_ATTR, transformCaseFunc);
		    }
		    if (cfg.ADD_URI_SAFE_ATTR) {
		      addToSet(URI_SAFE_ATTRIBUTES, cfg.ADD_URI_SAFE_ATTR, transformCaseFunc);
		    }
		    if (cfg.FORBID_CONTENTS) {
		      if (FORBID_CONTENTS === DEFAULT_FORBID_CONTENTS) {
		        FORBID_CONTENTS = clone(FORBID_CONTENTS);
		      }
		      addToSet(FORBID_CONTENTS, cfg.FORBID_CONTENTS, transformCaseFunc);
		    }
		    /* Add #text in case KEEP_CONTENT is set to true */
		    if (KEEP_CONTENT) {
		      ALLOWED_TAGS['#text'] = true;
		    }
		    /* Add html, head and body to ALLOWED_TAGS in case WHOLE_DOCUMENT is true */
		    if (WHOLE_DOCUMENT) {
		      addToSet(ALLOWED_TAGS, ['html', 'head', 'body']);
		    }
		    /* Add tbody to ALLOWED_TAGS in case tables are permitted, see #286, #365 */
		    if (ALLOWED_TAGS.table) {
		      addToSet(ALLOWED_TAGS, ['tbody']);
		      delete FORBID_TAGS.tbody;
		    }
		    if (cfg.TRUSTED_TYPES_POLICY) {
		      if (typeof cfg.TRUSTED_TYPES_POLICY.createHTML !== 'function') {
		        throw typeErrorCreate('TRUSTED_TYPES_POLICY configuration option must provide a "createHTML" hook.');
		      }
		      if (typeof cfg.TRUSTED_TYPES_POLICY.createScriptURL !== 'function') {
		        throw typeErrorCreate('TRUSTED_TYPES_POLICY configuration option must provide a "createScriptURL" hook.');
		      }
		      // Overwrite existing TrustedTypes policy.
		      trustedTypesPolicy = cfg.TRUSTED_TYPES_POLICY;
		      // Sign local variables required by `sanitize`.
		      emptyHTML = trustedTypesPolicy.createHTML('');
		    } else {
		      // Uninitialized policy, attempt to initialize the internal dompurify policy.
		      if (trustedTypesPolicy === undefined) {
		        trustedTypesPolicy = _createTrustedTypesPolicy(trustedTypes, currentScript);
		      }
		      // If creating the internal policy succeeded sign internal variables.
		      if (trustedTypesPolicy !== null && typeof emptyHTML === 'string') {
		        emptyHTML = trustedTypesPolicy.createHTML('');
		      }
		    }
		    // Prevent further manipulation of configuration.
		    // Not available in IE8, Safari 5, etc.
		    if (freeze) {
		      freeze(cfg);
		    }
		    CONFIG = cfg;
		  };
		  /* Keep track of all possible SVG and MathML tags
		   * so that we can perform the namespace checks
		   * correctly. */
		  const ALL_SVG_TAGS = addToSet({}, [...svg$1, ...svgFilters, ...svgDisallowed]);
		  const ALL_MATHML_TAGS = addToSet({}, [...mathMl$1, ...mathMlDisallowed]);
		  /**
		   * @param element a DOM element whose namespace is being checked
		   * @returns Return false if the element has a
		   *  namespace that a spec-compliant parser would never
		   *  return. Return true otherwise.
		   */
		  const _checkValidNamespace = function _checkValidNamespace(element) {
		    let parent = getParentNode(element);
		    // In JSDOM, if we're inside shadow DOM, then parentNode
		    // can be null. We just simulate parent in this case.
		    if (!parent || !parent.tagName) {
		      parent = {
		        namespaceURI: NAMESPACE,
		        tagName: 'template'
		      };
		    }
		    const tagName = stringToLowerCase(element.tagName);
		    const parentTagName = stringToLowerCase(parent.tagName);
		    if (!ALLOWED_NAMESPACES[element.namespaceURI]) {
		      return false;
		    }
		    if (element.namespaceURI === SVG_NAMESPACE) {
		      // The only way to switch from HTML namespace to SVG
		      // is via <svg>. If it happens via any other tag, then
		      // it should be killed.
		      if (parent.namespaceURI === HTML_NAMESPACE) {
		        return tagName === 'svg';
		      }
		      // The only way to switch from MathML to SVG is via`
		      // svg if parent is either <annotation-xml> or MathML
		      // text integration points.
		      if (parent.namespaceURI === MATHML_NAMESPACE) {
		        return tagName === 'svg' && (parentTagName === 'annotation-xml' || MATHML_TEXT_INTEGRATION_POINTS[parentTagName]);
		      }
		      // We only allow elements that are defined in SVG
		      // spec. All others are disallowed in SVG namespace.
		      return Boolean(ALL_SVG_TAGS[tagName]);
		    }
		    if (element.namespaceURI === MATHML_NAMESPACE) {
		      // The only way to switch from HTML namespace to MathML
		      // is via <math>. If it happens via any other tag, then
		      // it should be killed.
		      if (parent.namespaceURI === HTML_NAMESPACE) {
		        return tagName === 'math';
		      }
		      // The only way to switch from SVG to MathML is via
		      // <math> and HTML integration points
		      if (parent.namespaceURI === SVG_NAMESPACE) {
		        return tagName === 'math' && HTML_INTEGRATION_POINTS[parentTagName];
		      }
		      // We only allow elements that are defined in MathML
		      // spec. All others are disallowed in MathML namespace.
		      return Boolean(ALL_MATHML_TAGS[tagName]);
		    }
		    if (element.namespaceURI === HTML_NAMESPACE) {
		      // The only way to switch from SVG to HTML is via
		      // HTML integration points, and from MathML to HTML
		      // is via MathML text integration points
		      if (parent.namespaceURI === SVG_NAMESPACE && !HTML_INTEGRATION_POINTS[parentTagName]) {
		        return false;
		      }
		      if (parent.namespaceURI === MATHML_NAMESPACE && !MATHML_TEXT_INTEGRATION_POINTS[parentTagName]) {
		        return false;
		      }
		      // We disallow tags that are specific for MathML
		      // or SVG and should never appear in HTML namespace
		      return !ALL_MATHML_TAGS[tagName] && (COMMON_SVG_AND_HTML_ELEMENTS[tagName] || !ALL_SVG_TAGS[tagName]);
		    }
		    // For XHTML and XML documents that support custom namespaces
		    if (PARSER_MEDIA_TYPE === 'application/xhtml+xml' && ALLOWED_NAMESPACES[element.namespaceURI]) {
		      return true;
		    }
		    // The code should never reach this place (this means
		    // that the element somehow got namespace that is not
		    // HTML, SVG, MathML or allowed via ALLOWED_NAMESPACES).
		    // Return false just in case.
		    return false;
		  };
		  /**
		   * _forceRemove
		   *
		   * @param node a DOM node
		   */
		  const _forceRemove = function _forceRemove(node) {
		    arrayPush(DOMPurify.removed, {
		      element: node
		    });
		    try {
		      // eslint-disable-next-line unicorn/prefer-dom-node-remove
		      getParentNode(node).removeChild(node);
		    } catch (_) {
		      remove(node);
		    }
		  };
		  /**
		   * _removeAttribute
		   *
		   * @param name an Attribute name
		   * @param element a DOM node
		   */
		  const _removeAttribute = function _removeAttribute(name, element) {
		    try {
		      arrayPush(DOMPurify.removed, {
		        attribute: element.getAttributeNode(name),
		        from: element
		      });
		    } catch (_) {
		      arrayPush(DOMPurify.removed, {
		        attribute: null,
		        from: element
		      });
		    }
		    element.removeAttribute(name);
		    // We void attribute values for unremovable "is" attributes
		    if (name === 'is') {
		      if (RETURN_DOM || RETURN_DOM_FRAGMENT) {
		        try {
		          _forceRemove(element);
		        } catch (_) {}
		      } else {
		        try {
		          element.setAttribute(name, '');
		        } catch (_) {}
		      }
		    }
		  };
		  /**
		   * _initDocument
		   *
		   * @param dirty - a string of dirty markup
		   * @return a DOM, filled with the dirty markup
		   */
		  const _initDocument = function _initDocument(dirty) {
		    /* Create a HTML document */
		    let doc = null;
		    let leadingWhitespace = null;
		    if (FORCE_BODY) {
		      dirty = '<remove></remove>' + dirty;
		    } else {
		      /* If FORCE_BODY isn't used, leading whitespace needs to be preserved manually */
		      const matches = stringMatch(dirty, /^[\r\n\t ]+/);
		      leadingWhitespace = matches && matches[0];
		    }
		    if (PARSER_MEDIA_TYPE === 'application/xhtml+xml' && NAMESPACE === HTML_NAMESPACE) {
		      // Root of XHTML doc must contain xmlns declaration (see https://www.w3.org/TR/xhtml1/normative.html#strict)
		      dirty = '<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body>' + dirty + '</body></html>';
		    }
		    const dirtyPayload = trustedTypesPolicy ? trustedTypesPolicy.createHTML(dirty) : dirty;
		    /*
		     * Use the DOMParser API by default, fallback later if needs be
		     * DOMParser not work for svg when has multiple root element.
		     */
		    if (NAMESPACE === HTML_NAMESPACE) {
		      try {
		        doc = new DOMParser().parseFromString(dirtyPayload, PARSER_MEDIA_TYPE);
		      } catch (_) {}
		    }
		    /* Use createHTMLDocument in case DOMParser is not available */
		    if (!doc || !doc.documentElement) {
		      doc = implementation.createDocument(NAMESPACE, 'template', null);
		      try {
		        doc.documentElement.innerHTML = IS_EMPTY_INPUT ? emptyHTML : dirtyPayload;
		      } catch (_) {
		        // Syntax error if dirtyPayload is invalid xml
		      }
		    }
		    const body = doc.body || doc.documentElement;
		    if (dirty && leadingWhitespace) {
		      body.insertBefore(document.createTextNode(leadingWhitespace), body.childNodes[0] || null);
		    }
		    /* Work on whole document or just its body */
		    if (NAMESPACE === HTML_NAMESPACE) {
		      return getElementsByTagName.call(doc, WHOLE_DOCUMENT ? 'html' : 'body')[0];
		    }
		    return WHOLE_DOCUMENT ? doc.documentElement : body;
		  };
		  /**
		   * Creates a NodeIterator object that you can use to traverse filtered lists of nodes or elements in a document.
		   *
		   * @param root The root element or node to start traversing on.
		   * @return The created NodeIterator
		   */
		  const _createNodeIterator = function _createNodeIterator(root) {
		    return createNodeIterator.call(root.ownerDocument || root, root,
		    // eslint-disable-next-line no-bitwise
		    NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_COMMENT | NodeFilter.SHOW_TEXT | NodeFilter.SHOW_PROCESSING_INSTRUCTION | NodeFilter.SHOW_CDATA_SECTION, null);
		  };
		  /**
		   * _isClobbered
		   *
		   * @param element element to check for clobbering attacks
		   * @return true if clobbered, false if safe
		   */
		  const _isClobbered = function _isClobbered(element) {
		    return element instanceof HTMLFormElement && (typeof element.nodeName !== 'string' || typeof element.textContent !== 'string' || typeof element.removeChild !== 'function' || !(element.attributes instanceof NamedNodeMap) || typeof element.removeAttribute !== 'function' || typeof element.setAttribute !== 'function' || typeof element.namespaceURI !== 'string' || typeof element.insertBefore !== 'function' || typeof element.hasChildNodes !== 'function');
		  };
		  /**
		   * Checks whether the given object is a DOM node.
		   *
		   * @param value object to check whether it's a DOM node
		   * @return true is object is a DOM node
		   */
		  const _isNode = function _isNode(value) {
		    return typeof Node === 'function' && value instanceof Node;
		  };
		  function _executeHooks(hooks, currentNode, data) {
		    arrayForEach(hooks, hook => {
		      hook.call(DOMPurify, currentNode, data, CONFIG);
		    });
		  }
		  /**
		   * _sanitizeElements
		   *
		   * @protect nodeName
		   * @protect textContent
		   * @protect removeChild
		   * @param currentNode to check for permission to exist
		   * @return true if node was killed, false if left alive
		   */
		  const _sanitizeElements = function _sanitizeElements(currentNode) {
		    let content = null;
		    /* Execute a hook if present */
		    _executeHooks(hooks.beforeSanitizeElements, currentNode, null);
		    /* Check if element is clobbered or can clobber */
		    if (_isClobbered(currentNode)) {
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Now let's check the element's type and name */
		    const tagName = transformCaseFunc(currentNode.nodeName);
		    /* Execute a hook if present */
		    _executeHooks(hooks.uponSanitizeElement, currentNode, {
		      tagName,
		      allowedTags: ALLOWED_TAGS
		    });
		    /* Detect mXSS attempts abusing namespace confusion */
		    if (SAFE_FOR_XML && currentNode.hasChildNodes() && !_isNode(currentNode.firstElementChild) && regExpTest(/<[/\w!]/g, currentNode.innerHTML) && regExpTest(/<[/\w!]/g, currentNode.textContent)) {
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Remove any occurrence of processing instructions */
		    if (currentNode.nodeType === NODE_TYPE.progressingInstruction) {
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Remove any kind of possibly harmful comments */
		    if (SAFE_FOR_XML && currentNode.nodeType === NODE_TYPE.comment && regExpTest(/<[/\w]/g, currentNode.data)) {
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Remove element if anything forbids its presence */
		    if (!ALLOWED_TAGS[tagName] || FORBID_TAGS[tagName]) {
		      /* Check if we have a custom element to handle */
		      if (!FORBID_TAGS[tagName] && _isBasicCustomElement(tagName)) {
		        if (CUSTOM_ELEMENT_HANDLING.tagNameCheck instanceof RegExp && regExpTest(CUSTOM_ELEMENT_HANDLING.tagNameCheck, tagName)) {
		          return false;
		        }
		        if (CUSTOM_ELEMENT_HANDLING.tagNameCheck instanceof Function && CUSTOM_ELEMENT_HANDLING.tagNameCheck(tagName)) {
		          return false;
		        }
		      }
		      /* Keep content except for bad-listed elements */
		      if (KEEP_CONTENT && !FORBID_CONTENTS[tagName]) {
		        const parentNode = getParentNode(currentNode) || currentNode.parentNode;
		        const childNodes = getChildNodes(currentNode) || currentNode.childNodes;
		        if (childNodes && parentNode) {
		          const childCount = childNodes.length;
		          for (let i = childCount - 1; i >= 0; --i) {
		            const childClone = cloneNode(childNodes[i], true);
		            childClone.__removalCount = (currentNode.__removalCount || 0) + 1;
		            parentNode.insertBefore(childClone, getNextSibling(currentNode));
		          }
		        }
		      }
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Check whether element has a valid namespace */
		    if (currentNode instanceof Element && !_checkValidNamespace(currentNode)) {
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Make sure that older browsers don't get fallback-tag mXSS */
		    if ((tagName === 'noscript' || tagName === 'noembed' || tagName === 'noframes') && regExpTest(/<\/no(script|embed|frames)/i, currentNode.innerHTML)) {
		      _forceRemove(currentNode);
		      return true;
		    }
		    /* Sanitize element content to be template-safe */
		    if (SAFE_FOR_TEMPLATES && currentNode.nodeType === NODE_TYPE.text) {
		      /* Get the element's text content */
		      content = currentNode.textContent;
		      arrayForEach([MUSTACHE_EXPR, ERB_EXPR, TMPLIT_EXPR], expr => {
		        content = stringReplace(content, expr, ' ');
		      });
		      if (currentNode.textContent !== content) {
		        arrayPush(DOMPurify.removed, {
		          element: currentNode.cloneNode()
		        });
		        currentNode.textContent = content;
		      }
		    }
		    /* Execute a hook if present */
		    _executeHooks(hooks.afterSanitizeElements, currentNode, null);
		    return false;
		  };
		  /**
		   * _isValidAttribute
		   *
		   * @param lcTag Lowercase tag name of containing element.
		   * @param lcName Lowercase attribute name.
		   * @param value Attribute value.
		   * @return Returns true if `value` is valid, otherwise false.
		   */
		  // eslint-disable-next-line complexity
		  const _isValidAttribute = function _isValidAttribute(lcTag, lcName, value) {
		    /* Make sure attribute cannot clobber */
		    if (SANITIZE_DOM && (lcName === 'id' || lcName === 'name') && (value in document || value in formElement)) {
		      return false;
		    }
		    /* Allow valid data-* attributes: At least one character after "-"
		        (https://html.spec.whatwg.org/multipage/dom.html#embedding-custom-non-visible-data-with-the-data-*-attributes)
		        XML-compatible (https://html.spec.whatwg.org/multipage/infrastructure.html#xml-compatible and http://www.w3.org/TR/xml/#d0e804)
		        We don't need to check the value; it's always URI safe. */
		    if (ALLOW_DATA_ATTR && !FORBID_ATTR[lcName] && regExpTest(DATA_ATTR, lcName)) ; else if (ALLOW_ARIA_ATTR && regExpTest(ARIA_ATTR, lcName)) ; else if (!ALLOWED_ATTR[lcName] || FORBID_ATTR[lcName]) {
		      if (
		      // First condition does a very basic check if a) it's basically a valid custom element tagname AND
		      // b) if the tagName passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.tagNameCheck
		      // and c) if the attribute name passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.attributeNameCheck
		      _isBasicCustomElement(lcTag) && (CUSTOM_ELEMENT_HANDLING.tagNameCheck instanceof RegExp && regExpTest(CUSTOM_ELEMENT_HANDLING.tagNameCheck, lcTag) || CUSTOM_ELEMENT_HANDLING.tagNameCheck instanceof Function && CUSTOM_ELEMENT_HANDLING.tagNameCheck(lcTag)) && (CUSTOM_ELEMENT_HANDLING.attributeNameCheck instanceof RegExp && regExpTest(CUSTOM_ELEMENT_HANDLING.attributeNameCheck, lcName) || CUSTOM_ELEMENT_HANDLING.attributeNameCheck instanceof Function && CUSTOM_ELEMENT_HANDLING.attributeNameCheck(lcName)) ||
		      // Alternative, second condition checks if it's an `is`-attribute, AND
		      // the value passes whatever the user has configured for CUSTOM_ELEMENT_HANDLING.tagNameCheck
		      lcName === 'is' && CUSTOM_ELEMENT_HANDLING.allowCustomizedBuiltInElements && (CUSTOM_ELEMENT_HANDLING.tagNameCheck instanceof RegExp && regExpTest(CUSTOM_ELEMENT_HANDLING.tagNameCheck, value) || CUSTOM_ELEMENT_HANDLING.tagNameCheck instanceof Function && CUSTOM_ELEMENT_HANDLING.tagNameCheck(value))) ; else {
		        return false;
		      }
		      /* Check value is safe. First, is attr inert? If so, is safe */
		    } else if (URI_SAFE_ATTRIBUTES[lcName]) ; else if (regExpTest(IS_ALLOWED_URI$1, stringReplace(value, ATTR_WHITESPACE, ''))) ; else if ((lcName === 'src' || lcName === 'xlink:href' || lcName === 'href') && lcTag !== 'script' && stringIndexOf(value, 'data:') === 0 && DATA_URI_TAGS[lcTag]) ; else if (ALLOW_UNKNOWN_PROTOCOLS && !regExpTest(IS_SCRIPT_OR_DATA, stringReplace(value, ATTR_WHITESPACE, ''))) ; else if (value) {
		      return false;
		    } else ;
		    return true;
		  };
		  /**
		   * _isBasicCustomElement
		   * checks if at least one dash is included in tagName, and it's not the first char
		   * for more sophisticated checking see https://github.com/sindresorhus/validate-element-name
		   *
		   * @param tagName name of the tag of the node to sanitize
		   * @returns Returns true if the tag name meets the basic criteria for a custom element, otherwise false.
		   */
		  const _isBasicCustomElement = function _isBasicCustomElement(tagName) {
		    return tagName !== 'annotation-xml' && stringMatch(tagName, CUSTOM_ELEMENT);
		  };
		  /**
		   * _sanitizeAttributes
		   *
		   * @protect attributes
		   * @protect nodeName
		   * @protect removeAttribute
		   * @protect setAttribute
		   *
		   * @param currentNode to sanitize
		   */
		  const _sanitizeAttributes = function _sanitizeAttributes(currentNode) {
		    /* Execute a hook if present */
		    _executeHooks(hooks.beforeSanitizeAttributes, currentNode, null);
		    const {
		      attributes
		    } = currentNode;
		    /* Check if we have attributes; if not we might have a text node */
		    if (!attributes || _isClobbered(currentNode)) {
		      return;
		    }
		    const hookEvent = {
		      attrName: '',
		      attrValue: '',
		      keepAttr: true,
		      allowedAttributes: ALLOWED_ATTR,
		      forceKeepAttr: undefined
		    };
		    let l = attributes.length;
		    /* Go backwards over all attributes; safely remove bad ones */
		    while (l--) {
		      const attr = attributes[l];
		      const {
		        name,
		        namespaceURI,
		        value: attrValue
		      } = attr;
		      const lcName = transformCaseFunc(name);
		      const initValue = attrValue;
		      let value = name === 'value' ? initValue : stringTrim(initValue);
		      /* Execute a hook if present */
		      hookEvent.attrName = lcName;
		      hookEvent.attrValue = value;
		      hookEvent.keepAttr = true;
		      hookEvent.forceKeepAttr = undefined; // Allows developers to see this is a property they can set
		      _executeHooks(hooks.uponSanitizeAttribute, currentNode, hookEvent);
		      value = hookEvent.attrValue;
		      /* Full DOM Clobbering protection via namespace isolation,
		       * Prefix id and name attributes with `user-content-`
		       */
		      if (SANITIZE_NAMED_PROPS && (lcName === 'id' || lcName === 'name')) {
		        // Remove the attribute with this value
		        _removeAttribute(name, currentNode);
		        // Prefix the value and later re-create the attribute with the sanitized value
		        value = SANITIZE_NAMED_PROPS_PREFIX + value;
		      }
		      /* Work around a security issue with comments inside attributes */
		      if (SAFE_FOR_XML && regExpTest(/((--!?|])>)|<\/(style|title)/i, value)) {
		        _removeAttribute(name, currentNode);
		        continue;
		      }
		      /* Did the hooks approve of the attribute? */
		      if (hookEvent.forceKeepAttr) {
		        continue;
		      }
		      /* Did the hooks approve of the attribute? */
		      if (!hookEvent.keepAttr) {
		        _removeAttribute(name, currentNode);
		        continue;
		      }
		      /* Work around a security issue in jQuery 3.0 */
		      if (!ALLOW_SELF_CLOSE_IN_ATTR && regExpTest(/\/>/i, value)) {
		        _removeAttribute(name, currentNode);
		        continue;
		      }
		      /* Sanitize attribute content to be template-safe */
		      if (SAFE_FOR_TEMPLATES) {
		        arrayForEach([MUSTACHE_EXPR, ERB_EXPR, TMPLIT_EXPR], expr => {
		          value = stringReplace(value, expr, ' ');
		        });
		      }
		      /* Is `value` valid for this attribute? */
		      const lcTag = transformCaseFunc(currentNode.nodeName);
		      if (!_isValidAttribute(lcTag, lcName, value)) {
		        _removeAttribute(name, currentNode);
		        continue;
		      }
		      /* Handle attributes that require Trusted Types */
		      if (trustedTypesPolicy && typeof trustedTypes === 'object' && typeof trustedTypes.getAttributeType === 'function') {
		        if (namespaceURI) ; else {
		          switch (trustedTypes.getAttributeType(lcTag, lcName)) {
		            case 'TrustedHTML':
		              {
		                value = trustedTypesPolicy.createHTML(value);
		                break;
		              }
		            case 'TrustedScriptURL':
		              {
		                value = trustedTypesPolicy.createScriptURL(value);
		                break;
		              }
		          }
		        }
		      }
		      /* Handle invalid data-* attribute set by try-catching it */
		      if (value !== initValue) {
		        try {
		          if (namespaceURI) {
		            currentNode.setAttributeNS(namespaceURI, name, value);
		          } else {
		            /* Fallback to setAttribute() for browser-unrecognized namespaces e.g. "x-schema". */
		            currentNode.setAttribute(name, value);
		          }
		          if (_isClobbered(currentNode)) {
		            _forceRemove(currentNode);
		          } else {
		            arrayPop(DOMPurify.removed);
		          }
		        } catch (_) {
		          _removeAttribute(name, currentNode);
		        }
		      }
		    }
		    /* Execute a hook if present */
		    _executeHooks(hooks.afterSanitizeAttributes, currentNode, null);
		  };
		  /**
		   * _sanitizeShadowDOM
		   *
		   * @param fragment to iterate over recursively
		   */
		  const _sanitizeShadowDOM = function _sanitizeShadowDOM(fragment) {
		    let shadowNode = null;
		    const shadowIterator = _createNodeIterator(fragment);
		    /* Execute a hook if present */
		    _executeHooks(hooks.beforeSanitizeShadowDOM, fragment, null);
		    while (shadowNode = shadowIterator.nextNode()) {
		      /* Execute a hook if present */
		      _executeHooks(hooks.uponSanitizeShadowNode, shadowNode, null);
		      /* Sanitize tags and elements */
		      _sanitizeElements(shadowNode);
		      /* Check attributes next */
		      _sanitizeAttributes(shadowNode);
		      /* Deep shadow DOM detected */
		      if (shadowNode.content instanceof DocumentFragment) {
		        _sanitizeShadowDOM(shadowNode.content);
		      }
		    }
		    /* Execute a hook if present */
		    _executeHooks(hooks.afterSanitizeShadowDOM, fragment, null);
		  };
		  // eslint-disable-next-line complexity
		  DOMPurify.sanitize = function (dirty) {
		    let cfg = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
		    let body = null;
		    let importedNode = null;
		    let currentNode = null;
		    let returnNode = null;
		    /* Make sure we have a string to sanitize.
		      DO NOT return early, as this will return the wrong type if
		      the user has requested a DOM object rather than a string */
		    IS_EMPTY_INPUT = !dirty;
		    if (IS_EMPTY_INPUT) {
		      dirty = '<!-->';
		    }
		    /* Stringify, in case dirty is an object */
		    if (typeof dirty !== 'string' && !_isNode(dirty)) {
		      if (typeof dirty.toString === 'function') {
		        dirty = dirty.toString();
		        if (typeof dirty !== 'string') {
		          throw typeErrorCreate('dirty is not a string, aborting');
		        }
		      } else {
		        throw typeErrorCreate('toString is not a function');
		      }
		    }
		    /* Return dirty HTML if DOMPurify cannot run */
		    if (!DOMPurify.isSupported) {
		      return dirty;
		    }
		    /* Assign config vars */
		    if (!SET_CONFIG) {
		      _parseConfig(cfg);
		    }
		    /* Clean up removed elements */
		    DOMPurify.removed = [];
		    /* Check if dirty is correctly typed for IN_PLACE */
		    if (typeof dirty === 'string') {
		      IN_PLACE = false;
		    }
		    if (IN_PLACE) {
		      /* Do some early pre-sanitization to avoid unsafe root nodes */
		      if (dirty.nodeName) {
		        const tagName = transformCaseFunc(dirty.nodeName);
		        if (!ALLOWED_TAGS[tagName] || FORBID_TAGS[tagName]) {
		          throw typeErrorCreate('root node is forbidden and cannot be sanitized in-place');
		        }
		      }
		    } else if (dirty instanceof Node) {
		      /* If dirty is a DOM element, append to an empty document to avoid
		         elements being stripped by the parser */
		      body = _initDocument('<!---->');
		      importedNode = body.ownerDocument.importNode(dirty, true);
		      if (importedNode.nodeType === NODE_TYPE.element && importedNode.nodeName === 'BODY') {
		        /* Node is already a body, use as is */
		        body = importedNode;
		      } else if (importedNode.nodeName === 'HTML') {
		        body = importedNode;
		      } else {
		        // eslint-disable-next-line unicorn/prefer-dom-node-append
		        body.appendChild(importedNode);
		      }
		    } else {
		      /* Exit directly if we have nothing to do */
		      if (!RETURN_DOM && !SAFE_FOR_TEMPLATES && !WHOLE_DOCUMENT &&
		      // eslint-disable-next-line unicorn/prefer-includes
		      dirty.indexOf('<') === -1) {
		        return trustedTypesPolicy && RETURN_TRUSTED_TYPE ? trustedTypesPolicy.createHTML(dirty) : dirty;
		      }
		      /* Initialize the document to work on */
		      body = _initDocument(dirty);
		      /* Check we have a DOM node from the data */
		      if (!body) {
		        return RETURN_DOM ? null : RETURN_TRUSTED_TYPE ? emptyHTML : '';
		      }
		    }
		    /* Remove first element node (ours) if FORCE_BODY is set */
		    if (body && FORCE_BODY) {
		      _forceRemove(body.firstChild);
		    }
		    /* Get node iterator */
		    const nodeIterator = _createNodeIterator(IN_PLACE ? dirty : body);
		    /* Now start iterating over the created document */
		    while (currentNode = nodeIterator.nextNode()) {
		      /* Sanitize tags and elements */
		      _sanitizeElements(currentNode);
		      /* Check attributes next */
		      _sanitizeAttributes(currentNode);
		      /* Shadow DOM detected, sanitize it */
		      if (currentNode.content instanceof DocumentFragment) {
		        _sanitizeShadowDOM(currentNode.content);
		      }
		    }
		    /* If we sanitized `dirty` in-place, return it. */
		    if (IN_PLACE) {
		      return dirty;
		    }
		    /* Return sanitized string or DOM */
		    if (RETURN_DOM) {
		      if (RETURN_DOM_FRAGMENT) {
		        returnNode = createDocumentFragment.call(body.ownerDocument);
		        while (body.firstChild) {
		          // eslint-disable-next-line unicorn/prefer-dom-node-append
		          returnNode.appendChild(body.firstChild);
		        }
		      } else {
		        returnNode = body;
		      }
		      if (ALLOWED_ATTR.shadowroot || ALLOWED_ATTR.shadowrootmode) {
		        /*
		          AdoptNode() is not used because internal state is not reset
		          (e.g. the past names map of a HTMLFormElement), this is safe
		          in theory but we would rather not risk another attack vector.
		          The state that is cloned by importNode() is explicitly defined
		          by the specs.
		        */
		        returnNode = importNode.call(originalDocument, returnNode, true);
		      }
		      return returnNode;
		    }
		    let serializedHTML = WHOLE_DOCUMENT ? body.outerHTML : body.innerHTML;
		    /* Serialize doctype if allowed */
		    if (WHOLE_DOCUMENT && ALLOWED_TAGS['!doctype'] && body.ownerDocument && body.ownerDocument.doctype && body.ownerDocument.doctype.name && regExpTest(DOCTYPE_NAME, body.ownerDocument.doctype.name)) {
		      serializedHTML = '<!DOCTYPE ' + body.ownerDocument.doctype.name + '>\n' + serializedHTML;
		    }
		    /* Sanitize final string template-safe */
		    if (SAFE_FOR_TEMPLATES) {
		      arrayForEach([MUSTACHE_EXPR, ERB_EXPR, TMPLIT_EXPR], expr => {
		        serializedHTML = stringReplace(serializedHTML, expr, ' ');
		      });
		    }
		    return trustedTypesPolicy && RETURN_TRUSTED_TYPE ? trustedTypesPolicy.createHTML(serializedHTML) : serializedHTML;
		  };
		  DOMPurify.setConfig = function () {
		    let cfg = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
		    _parseConfig(cfg);
		    SET_CONFIG = true;
		  };
		  DOMPurify.clearConfig = function () {
		    CONFIG = null;
		    SET_CONFIG = false;
		  };
		  DOMPurify.isValidAttribute = function (tag, attr, value) {
		    /* Initialize shared config vars if necessary. */
		    if (!CONFIG) {
		      _parseConfig({});
		    }
		    const lcTag = transformCaseFunc(tag);
		    const lcName = transformCaseFunc(attr);
		    return _isValidAttribute(lcTag, lcName, value);
		  };
		  DOMPurify.addHook = function (entryPoint, hookFunction) {
		    if (typeof hookFunction !== 'function') {
		      return;
		    }
		    arrayPush(hooks[entryPoint], hookFunction);
		  };
		  DOMPurify.removeHook = function (entryPoint, hookFunction) {
		    if (hookFunction !== undefined) {
		      const index = arrayLastIndexOf(hooks[entryPoint], hookFunction);
		      return index === -1 ? undefined : arraySplice(hooks[entryPoint], index, 1)[0];
		    }
		    return arrayPop(hooks[entryPoint]);
		  };
		  DOMPurify.removeHooks = function (entryPoint) {
		    hooks[entryPoint] = [];
		  };
		  DOMPurify.removeAllHooks = function () {
		    hooks = _createHooksMap();
		  };
		  return DOMPurify;
		}
		var purify = createDOMPurify();

		purify_cjs = purify;
		
		return purify_cjs;
	}

	(function(module, exports$1) {
	  !function(e, t) {
	    t(exports$1, requirePurify_cjs()) ;
	  }(commonjsGlobal, function(e, t) {
	    function i(e2, t2, i2, n2) {
	      return new (i2 || (i2 = Promise))(function(r2, s2) {
	        function o2(e3) {
	          try {
	            l2(n2.next(e3));
	          } catch (e4) {
	            s2(e4);
	          }
	        }
	        function a2(e3) {
	          try {
	            l2(n2.throw(e3));
	          } catch (e4) {
	            s2(e4);
	          }
	        }
	        function l2(e3) {
	          var t3;
	          e3.done ? r2(e3.value) : (t3 = e3.value, t3 instanceof i2 ? t3 : new i2(function(e4) {
	            e4(t3);
	          })).then(o2, a2);
	        }
	        l2((n2 = n2.apply(e2, t2 || [])).next());
	      });
	    }
	    class n {
	      constructor() {
	        this.BasePath = "", this.ImmediatePrint = false, this.ContentOnly = false, this.UseSVG = false, this.enableSearch = false, this.enableAccessibility = false, this.contentTabIndex = 0;
	      }
	    }
	    class r {
	      constructor() {
	        this.from = "", this.to = "", this.cc = "", this.subject = "", this.format = "", this.body = "";
	      }
	    }
	    class s {
	      constructor(e2, t2) {
	        this.cancel = false, this.element = e2, this.action = t2;
	      }
	    }
	    class o {
	      constructor(e2, t2) {
	        this.id = "", this.type = "", this.id = e2, this.type = t2;
	      }
	    }
	    class a {
	      constructor(e2, t2) {
	        this.isCancelled = false, this.format = "", this.deviceInfo = e2, this.format = t2;
	      }
	    }
	    class l {
	      constructor(e2, t2) {
	        this.handled = false, this.deviceInfo = e2, this.format = t2;
	      }
	    }
	    class h {
	      constructor(e2, t2, i2) {
	        this.handled = false, this.url = e2, this.format = t2, this.windowOpenTarget = i2;
	      }
	    }
	    class c {
	      constructor(e2) {
	        this.handled = false, this.deviceInfo = e2;
	      }
	    }
	    class d {
	      constructor(e2) {
	        this.handled = false, this.url = e2;
	      }
	    }
	    class u {
	      constructor(e2, t2) {
	        this.page = e2, this.reportDocumentId = t2;
	      }
	    }
	    class p {
	      constructor(e2, t2, i2, n2 = null) {
	        this.element = e2, this.text = t2, this.title = i2, this.eventArgs = n2;
	      }
	    }
	    class g {
	      constructor(e2, t2) {
	        this._responseText = e2, this._error = t2;
	        try {
	          this._responseJSON = JSON.parse(e2);
	        } catch (e3) {
	          this._responseJSON = null;
	        }
	      }
	      get responseText() {
	        return this._responseText;
	      }
	      get responseJSON() {
	        return this._responseJSON;
	      }
	      get error() {
	        return this._error;
	      }
	      get exceptionMessage() {
	        var e2, t2;
	        return (null === (e2 = this.responseJSON) || void 0 === e2 ? void 0 : e2.exceptionMessage) || (null === (t2 = this.responseJSON) || void 0 === t2 ? void 0 : t2.ExceptionMessage);
	      }
	    }
	    function m(e2, t2 = false, i2 = false) {
	      let n2 = { Accept: "application/json, text/javascript, */*; q=0.01" };
	      return t2 && (n2["Content-Type"] = i2 ? "application/x-www-form-urlencoded; charset=UTF-8" : "application/json; charset=UTF-8"), e2 && (n2.authorization = "Bearer " + e2), n2;
	    }
	    function f(e2) {
	      return i(this, void 0, void 0, function* () {
	        if (!e2.ok) {
	          let t2 = yield e2.text(), i2 = new g(t2, e2.statusText);
	          return Promise.reject(i2);
	        }
	        if (204 == e2.status)
	          return Promise.resolve();
	        return (e2.headers.get("content-type") || "").includes("application/json") ? e2.json() : e2.text();
	      });
	    }
	    function v(e2, t2 = {}, i2 = "", n2 = false) {
	      return fetch(e2, { method: "POST", headers: m(i2, true, n2), body: n2 ? t2 : JSON.stringify(t2) }).then(f);
	    }
	    class P {
	      authenticatePromise() {
	        return Promise.resolve("");
	      }
	    }
	    class C {
	      constructor(e2) {
	        this.connectionConfig = e2;
	      }
	      authenticatePromise() {
	        var e2, t2;
	        if (this.connectionConfig && this.connectionConfig.tokenUrl && (this.connectionConfig.username || this.connectionConfig.password)) {
	          let i2 = `grant_type=password&username=${encodeURIComponent((null === (e2 = this.connectionConfig) || void 0 === e2 ? void 0 : e2.username) || "")}&password=${encodeURIComponent((null === (t2 = this.connectionConfig) || void 0 === t2 ? void 0 : t2.password) || "")}`;
	          return v(this.connectionConfig.tokenUrl, i2, "", true).then((e3) => (e3.expiresAt = Date.now() + 1e3 * e3.expiresIn, e3));
	        }
	        return Promise.reject("Failed to connect to Report Server with user credentials. Are you missing the reportServer.url, reportServer.username or reportServer.password values?");
	      }
	    }
	    class y {
	      constructor(e2) {
	        this.connectionConfig = e2;
	      }
	      authenticatePromise(e2, t2) {
	        return e2 ? v(this.connectionConfig.refreshTokenUrl, { RefreshToken: t2 }).then((e3) => (e3.expiresAt = Date.now() + 1e3 * e3.expiresIn, e3)) : this.connectionConfig && this.connectionConfig.personalTokenUrl && this.connectionConfig.getPersonalAccessToken ? this.connectionConfig.getPersonalAccessToken().then((e3) => v(this.connectionConfig.personalTokenUrl, e3).then((e4) => (e4.expiresAt = Date.now() + 1e3 * e4.expiresIn, e4))) : Promise.reject("Failed to connect to Report Server with personal access token. Are you missing the reportServer.url or reportServer.getPersonalAccessToken values?");
	      }
	    }
	    var S, I;
	    e.AuthType = void 0, (S = e.AuthType || (e.AuthType = {}))[S.None = 0] = "None", S[S.Basic = 1] = "Basic", S[S.PersonalToken = 2] = "PersonalToken";
	    class b {
	      constructor(e2) {
	        this.baseUrl = null == e2 ? void 0 : e2.replace(/\/$/, "");
	      }
	    }
	    class w extends b {
	      constructor(t2) {
	        super(t2), this.authType = e.AuthType.None, this.serviceUrl = this.baseUrl;
	      }
	    }
	    class T extends b {
	      constructor(t2) {
	        super(t2), this.authType = e.AuthType.None, this.serviceUrl = this.baseUrl + "/api/reports";
	      }
	    }
	    class A extends T {
	      constructor(t2, i2, n2) {
	        super(t2), this.authType = e.AuthType.Basic, this.username = i2, this.password = n2, this.tokenUrl = this.baseUrl + "/Token";
	      }
	    }
	    class R extends T {
	      constructor(t2, i2) {
	        super(t2), this.authType = e.AuthType.PersonalToken, this.getPersonalAccessToken = i2, this.personalTokenUrl = this.baseUrl + "/PersonalToken", this.refreshTokenUrl = this.baseUrl + "/refresh";
	      }
	    }
	    function E() {
	    }
	    function M() {
	      M.init.call(this);
	    }
	    function x(e2) {
	      return void 0 === e2._maxListeners ? M.defaultMaxListeners : e2._maxListeners;
	    }
	    function k(e2, t2, i2, n2) {
	      var r2, s2, o2, a2;
	      if ("function" != typeof i2)
	        throw new TypeError('"listener" argument must be a function');
	      if ((s2 = e2._events) ? (s2.newListener && (e2.emit("newListener", t2, i2.listener ? i2.listener : i2), s2 = e2._events), o2 = s2[t2]) : (s2 = e2._events = new E(), e2._eventsCount = 0), o2) {
	        if ("function" == typeof o2 ? o2 = s2[t2] = n2 ? [i2, o2] : [o2, i2] : n2 ? o2.unshift(i2) : o2.push(i2), !o2.warned && (r2 = x(e2)) && r2 > 0 && o2.length > r2) {
	          o2.warned = true;
	          var l2 = new Error("Possible EventEmitter memory leak detected. " + o2.length + " " + t2 + " listeners added. Use emitter.setMaxListeners() to increase limit");
	          l2.name = "MaxListenersExceededWarning", l2.emitter = e2, l2.type = t2, l2.count = o2.length, a2 = l2, "function" == typeof console.warn ? console.warn(a2) : console.log(a2);
	        }
	      } else
	        o2 = s2[t2] = i2, ++e2._eventsCount;
	      return e2;
	    }
	    function L(e2, t2, i2) {
	      var n2 = false;
	      function r2() {
	        e2.removeListener(t2, r2), n2 || (n2 = true, i2.apply(e2, arguments));
	      }
	      return r2.listener = i2, r2;
	    }
	    function N(e2) {
	      var t2 = this._events;
	      if (t2) {
	        var i2 = t2[e2];
	        if ("function" == typeof i2)
	          return 1;
	        if (i2)
	          return i2.length;
	      }
	      return 0;
	    }
	    function D(e2, t2) {
	      for (var i2 = new Array(t2); t2--; )
	        i2[t2] = e2[t2];
	      return i2;
	    }
	    E.prototype = /* @__PURE__ */ Object.create(null), M.EventEmitter = M, M.usingDomains = false, M.prototype.domain = void 0, M.prototype._events = void 0, M.prototype._maxListeners = void 0, M.defaultMaxListeners = 10, M.init = function() {
	      this.domain = null, M.usingDomains && (!I.active || this instanceof I.Domain || (this.domain = I.active)), this._events && this._events !== Object.getPrototypeOf(this)._events || (this._events = new E(), this._eventsCount = 0), this._maxListeners = this._maxListeners || void 0;
	    }, M.prototype.setMaxListeners = function(e2) {
	      if ("number" != typeof e2 || e2 < 0 || isNaN(e2))
	        throw new TypeError('"n" argument must be a positive number');
	      return this._maxListeners = e2, this;
	    }, M.prototype.getMaxListeners = function() {
	      return x(this);
	    }, M.prototype.emit = function(e2) {
	      var t2, i2, n2, r2, s2, o2, a2, l2 = "error" === e2;
	      if (o2 = this._events)
	        l2 = l2 && null == o2.error;
	      else if (!l2)
	        return false;
	      if (a2 = this.domain, l2) {
	        if (t2 = arguments[1], !a2) {
	          if (t2 instanceof Error)
	            throw t2;
	          var h2 = new Error('Uncaught, unspecified "error" event. (' + t2 + ")");
	          throw h2.context = t2, h2;
	        }
	        return t2 || (t2 = new Error('Uncaught, unspecified "error" event')), t2.domainEmitter = this, t2.domain = a2, t2.domainThrown = false, a2.emit("error", t2), false;
	      }
	      if (!(i2 = o2[e2]))
	        return false;
	      var c2 = "function" == typeof i2;
	      switch (n2 = arguments.length) {
	        case 1:
	          !function(e3, t3, i3) {
	            if (t3)
	              e3.call(i3);
	            else
	              for (var n3 = e3.length, r3 = D(e3, n3), s3 = 0; s3 < n3; ++s3)
	                r3[s3].call(i3);
	          }(i2, c2, this);
	          break;
	        case 2:
	          !function(e3, t3, i3, n3) {
	            if (t3)
	              e3.call(i3, n3);
	            else
	              for (var r3 = e3.length, s3 = D(e3, r3), o3 = 0; o3 < r3; ++o3)
	                s3[o3].call(i3, n3);
	          }(i2, c2, this, arguments[1]);
	          break;
	        case 3:
	          !function(e3, t3, i3, n3, r3) {
	            if (t3)
	              e3.call(i3, n3, r3);
	            else
	              for (var s3 = e3.length, o3 = D(e3, s3), a3 = 0; a3 < s3; ++a3)
	                o3[a3].call(i3, n3, r3);
	          }(i2, c2, this, arguments[1], arguments[2]);
	          break;
	        case 4:
	          !function(e3, t3, i3, n3, r3, s3) {
	            if (t3)
	              e3.call(i3, n3, r3, s3);
	            else
	              for (var o3 = e3.length, a3 = D(e3, o3), l3 = 0; l3 < o3; ++l3)
	                a3[l3].call(i3, n3, r3, s3);
	          }(i2, c2, this, arguments[1], arguments[2], arguments[3]);
	          break;
	        default:
	          for (r2 = new Array(n2 - 1), s2 = 1; s2 < n2; s2++)
	            r2[s2 - 1] = arguments[s2];
	          !function(e3, t3, i3, n3) {
	            if (t3)
	              e3.apply(i3, n3);
	            else
	              for (var r3 = e3.length, s3 = D(e3, r3), o3 = 0; o3 < r3; ++o3)
	                s3[o3].apply(i3, n3);
	          }(i2, c2, this, r2);
	      }
	      return true;
	    }, M.prototype.addListener = function(e2, t2) {
	      return k(this, e2, t2, false);
	    }, M.prototype.on = M.prototype.addListener, M.prototype.prependListener = function(e2, t2) {
	      return k(this, e2, t2, true);
	    }, M.prototype.once = function(e2, t2) {
	      if ("function" != typeof t2)
	        throw new TypeError('"listener" argument must be a function');
	      return this.on(e2, L(this, e2, t2)), this;
	    }, M.prototype.prependOnceListener = function(e2, t2) {
	      if ("function" != typeof t2)
	        throw new TypeError('"listener" argument must be a function');
	      return this.prependListener(e2, L(this, e2, t2)), this;
	    }, M.prototype.removeListener = function(e2, t2) {
	      var i2, n2, r2, s2, o2;
	      if ("function" != typeof t2)
	        throw new TypeError('"listener" argument must be a function');
	      if (!(n2 = this._events))
	        return this;
	      if (!(i2 = n2[e2]))
	        return this;
	      if (i2 === t2 || i2.listener && i2.listener === t2)
	        0 === --this._eventsCount ? this._events = new E() : (delete n2[e2], n2.removeListener && this.emit("removeListener", e2, i2.listener || t2));
	      else if ("function" != typeof i2) {
	        for (r2 = -1, s2 = i2.length; s2-- > 0; )
	          if (i2[s2] === t2 || i2[s2].listener && i2[s2].listener === t2) {
	            o2 = i2[s2].listener, r2 = s2;
	            break;
	          }
	        if (r2 < 0)
	          return this;
	        if (1 === i2.length) {
	          if (i2[0] = void 0, 0 === --this._eventsCount)
	            return this._events = new E(), this;
	          delete n2[e2];
	        } else
	          !function(e3, t3) {
	            for (var i3 = t3, n3 = i3 + 1, r3 = e3.length; n3 < r3; i3 += 1, n3 += 1)
	              e3[i3] = e3[n3];
	            e3.pop();
	          }(i2, r2);
	        n2.removeListener && this.emit("removeListener", e2, o2 || t2);
	      }
	      return this;
	    }, M.prototype.off = function(e2, t2) {
	      return this.removeListener(e2, t2);
	    }, M.prototype.removeAllListeners = function(e2) {
	      var t2, i2;
	      if (!(i2 = this._events))
	        return this;
	      if (!i2.removeListener)
	        return 0 === arguments.length ? (this._events = new E(), this._eventsCount = 0) : i2[e2] && (0 === --this._eventsCount ? this._events = new E() : delete i2[e2]), this;
	      if (0 === arguments.length) {
	        for (var n2, r2 = Object.keys(i2), s2 = 0; s2 < r2.length; ++s2)
	          "removeListener" !== (n2 = r2[s2]) && this.removeAllListeners(n2);
	        return this.removeAllListeners("removeListener"), this._events = new E(), this._eventsCount = 0, this;
	      }
	      if ("function" == typeof (t2 = i2[e2]))
	        this.removeListener(e2, t2);
	      else if (t2)
	        do {
	          this.removeListener(e2, t2[t2.length - 1]);
	        } while (t2[0]);
	      return this;
	    }, M.prototype.listeners = function(e2) {
	      var t2, i2 = this._events;
	      return i2 && (t2 = i2[e2]) ? "function" == typeof t2 ? [t2.listener || t2] : function(e3) {
	        for (var t3 = new Array(e3.length), i3 = 0; i3 < t3.length; ++i3)
	          t3[i3] = e3[i3].listener || e3[i3];
	        return t3;
	      }(t2) : [];
	    }, M.listenerCount = function(e2, t2) {
	      return "function" == typeof e2.listenerCount ? e2.listenerCount(t2) : N.call(e2, t2);
	    }, M.prototype.listenerCount = N, M.prototype.eventNames = function() {
	      return this._eventsCount > 0 ? Reflect.ownKeys(this._events) : [];
	    };
	    const F = "function" == typeof Symbol ? Symbol.for("--[[await-event-emitter]]--") : "--[[await-event-emitter]]--";
	    function O(e2) {
	      if ("string" != typeof e2 && "symbol" != typeof e2)
	        throw new TypeError("type is not type of string or symbol!");
	    }
	    function V(e2) {
	      if ("function" != typeof e2)
	        throw new TypeError("fn is not type of Function!");
	    }
	    function z(e2) {
	      return { [F]: "always", fn: e2 };
	    }
	    function _(e2) {
	      return { [F]: "once", fn: e2 };
	    }
	    class U {
	      constructor() {
	        this._events = {};
	      }
	      addListener(e2, t2) {
	        return this.on(e2, t2);
	      }
	      on(e2, t2) {
	        return O(e2), V(t2), this._events[e2] = this._events[e2] || [], this._events[e2].push(z(t2)), this;
	      }
	      prependListener(e2, t2) {
	        return this.prepend(e2, t2);
	      }
	      prepend(e2, t2) {
	        return O(e2), V(t2), this._events[e2] = this._events[e2] || [], this._events[e2].unshift(z(t2)), this;
	      }
	      prependOnceListener(e2, t2) {
	        return this.prependOnce(e2, t2);
	      }
	      prependOnce(e2, t2) {
	        return O(e2), V(t2), this._events[e2] = this._events[e2] || [], this._events[e2].unshift(_(t2)), this;
	      }
	      listeners(e2) {
	        return (this._events[e2] || []).map((e3) => e3.fn);
	      }
	      once(e2, t2) {
	        return O(e2), V(t2), this._events[e2] = this._events[e2] || [], this._events[e2].push(_(t2)), this;
	      }
	      removeAllListeners() {
	        this._events = {};
	      }
	      off(e2, t2) {
	        return this.removeListener(e2, t2);
	      }
	      removeListener(e2, t2) {
	        O(e2);
	        const i2 = this.listeners(e2);
	        if ("function" == typeof t2) {
	          let n2 = -1, r2 = false;
	          for (; (n2 = i2.indexOf(t2)) >= 0; )
	            i2.splice(n2, 1), this._events[e2].splice(n2, 1), r2 = true;
	          return r2;
	        }
	        return delete this._events[e2];
	      }
	      emit(e2, ...t2) {
	        return i(this, void 0, void 0, function* () {
	          O(e2);
	          const i2 = this.listeners(e2), n2 = [];
	          if (i2 && i2.length) {
	            for (let r2 = 0; r2 < i2.length; r2++) {
	              const s2 = i2[r2], o2 = s2.apply(this, t2);
	              o2 instanceof Promise && (yield o2), this._events[e2] && this._events[e2][r2] && "once" === this._events[e2][r2][F] && n2.push(s2);
	            }
	            return n2.forEach((t3) => this.removeListener(e2, t3)), true;
	          }
	          return false;
	        });
	      }
	      emitSync(e2, ...t2) {
	        O(e2);
	        const i2 = this.listeners(e2), n2 = [];
	        if (i2 && i2.length) {
	          for (let r2 = 0; r2 < i2.length; r2++) {
	            const s2 = i2[r2];
	            s2.apply(this, t2), this._events[e2] && this._events[e2][r2] && "once" === this._events[e2][r2][F] && n2.push(s2);
	          }
	          return n2.forEach((t3) => this.removeListener(e2, t3)), true;
	        }
	        return false;
	      }
	    }
	    class $ {
	      constructor() {
	        this.eventEmitter = new M(), this.awaitEventEmitter = new U();
	      }
	      on(e2, t2) {
	        return this.eventEmitter.on(e2, t2), this;
	      }
	      emit(e2, ...t2) {
	        this.eventEmitter.emit(e2, ...t2);
	      }
	      onAsync(e2, t2) {
	        return this.awaitEventEmitter.on(e2, t2), this;
	      }
	      emitAsync(e2, ...t2) {
	        return i(this, void 0, void 0, function* () {
	          yield this.awaitEventEmitter.emit(e2, ...t2);
	        });
	      }
	    }
	    class H {
	      hasPdfPlugin() {
	        let e2 = ["AcroPDF.PDF.1", "PDF.PdfCtrl.6", "PDF.PdfCtrl.5"];
	        for (let t2 of e2)
	          try {
	            let e3 = new ActiveXObject(t2);
	            if (e3)
	              return null !== e3;
	          } catch (e3) {
	          }
	        return false;
	      }
	    }
	    class B {
	      hasPdfPlugin() {
	        let e2 = /Firefox[/\s](\d+\.\d+)/.exec(navigator.userAgent);
	        if (null !== e2 && e2.length > 1) {
	          if (parseFloat(e2[1]) >= 19)
	            return false;
	        }
	        let t2 = navigator.mimeTypes["application/pdf"], i2 = null !== t2 ? t2.enabledPlugin : null;
	        if (i2) {
	          let e3 = i2.description;
	          return -1 !== e3.indexOf("Adobe") && (-1 === e3.indexOf("Version") || parseFloat(e3.split("Version")[1]) >= 6);
	        }
	        return false;
	      }
	    }
	    class q {
	      constructor(e2) {
	        this.defaultPlugin = e2;
	      }
	      hasPdfPlugin() {
	        for (let e2 of navigator.plugins)
	          if (e2.name === this.defaultPlugin || "Adobe Acrobat" === e2.name)
	            return true;
	        return false;
	      }
	    }
	    class W {
	      hasPdfPlugin() {
	        return false;
	      }
	    }
	    function j() {
	      return window.navigator && window.navigator.msSaveOrOpenBlob;
	    }
	    class J {
	      constructor() {
	        this.hasPdfPlugin = false, this.iframe = null, this.hasPdfPlugin = function() {
	          if (window.navigator) {
	            let e2 = window.navigator.userAgent.toLowerCase();
	            return e2.indexOf("msie") > -1 || e2.indexOf("mozilla") > -1 && e2.indexOf("trident") > -1 ? new H() : e2.indexOf("firefox") > -1 ? new B() : e2.indexOf("edg/") > -1 ? new q("Microsoft Edge PDF Plugin") : e2.indexOf("chrome") > -1 ? new q("Chrome PDF Viewer") : e2.indexOf("safari") > -1 ? new q("WebKit built-in PDF") : new W();
	          }
	          return new W();
	        }().hasPdfPlugin(), this.isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
	      }
	      destroy() {
	        this.iframe = null;
	      }
	      printDesktop(e2) {
	        let t2 = null;
	        if (this.iframe || (this.iframe = document.createElement("iframe"), this.iframe.style.display = "none", this.iframe.onload = () => {
	          var e3, i3;
	          try {
	            null === (i3 = null === (e3 = this.iframe) || void 0 === e3 ? void 0 : e3.contentDocument) || void 0 === i3 || i3.execCommand("print", true);
	          } finally {
	            t2 && (window.URL || window.webkitURL).revokeObjectURL(t2);
	          }
	        }), function(e3) {
	          let t3 = window.location, i3 = document.createElement("a");
	          return i3.setAttribute("href", e3), "" == i3.host && (i3.href = i3.href), t3.hostname === i3.hostname && t3.protocol === i3.protocol && t3.port === i3.port;
	        }(e2) && j())
	          return this.iframe.src = e2, void document.body.appendChild(this.iframe);
	        let i2 = new XMLHttpRequest(), n2 = this;
	        i2.open("GET", e2, true), i2.responseType = "arraybuffer", i2.onload = function() {
	          if (200 === this.status) {
	            let e3 = new Blob([this.response], { type: "application/pdf" });
	            j() ? window.navigator.msSaveOrOpenBlob(e3) : (t2 = (window.URL || window.webkitURL).createObjectURL(e3), null != n2.iframe && (n2.iframe.src = t2, document.body.appendChild(n2.iframe)));
	          } else
	            console.log("Could not retrieve remote PDF document.");
	        }, i2.send();
	      }
	      printMobile(e2) {
	        window.open(e2, "_self");
	      }
	      print(e2) {
	        this.isMobile ? this.printMobile(e2) : this.printDesktop(e2);
	      }
	      getDirectPrintState() {
	        return this.hasPdfPlugin;
	      }
	    }
	    function G(e2) {
	      return 1e3 * e2;
	    }
	    class Z {
	      constructor(e2, t2, i2) {
	        if (this.pingMilliseconds = 0, !e2)
	          throw "Error";
	        this.serviceClient = e2, this.clientID = t2, this.initSessionTimeout(i2);
	      }
	      initSessionTimeout(e2) {
	        if (!isFinite(e2))
	          throw "sessionTimeoutSeconds must be finite";
	        this.pingMilliseconds = e2 <= 120 ? G(e2) / 2 : G(e2 - 60);
	      }
	      start() {
	        this.pingMilliseconds <= 0 || (this.interval = setInterval(() => {
	          this.serviceClient.keepClientAlive(this.clientID);
	        }, this.pingMilliseconds));
	      }
	      stop() {
	        this.interval && (clearInterval(this.interval), this.interval = null);
	      }
	    }
	    var K, X, Y, Q, ee;
	    function te(e2, t2 = "", i2 = "") {
	      let n2 = document.createElement(e2);
	      return t2 && (n2.id = t2), ie(n2, i2), n2;
	    }
	    function ie(e2, t2) {
	      if ("" === t2 || !e2)
	        return;
	      let i2 = t2.trim().split(" ");
	      i2 = i2.filter((e3) => "" !== e3.trim()), e2.classList.add(...i2);
	    }
	    function ne(e2, t2) {
	      if ("" === t2 || !e2)
	        return;
	      let i2 = t2.trim().split(" ");
	      i2 = i2.filter((e3) => "" !== e3.trim()), e2.classList.remove(...i2);
	    }
	    function re(e2, t2) {
	      return e2.classList.contains(t2);
	    }
	    function se(e2) {
	      return e2.offsetParent;
	    }
	    function oe(e2) {
	      return parseInt(e2, 10) || 0;
	    }
	    function ae(e2, t2, i2, n2 = 0, r2 = 0) {
	      let s2 = `${n2 = n2 || 0} ${r2 = r2 || 0}`;
	      !function(e3, t3) {
	        e3.style.setProperty("transform", t3), e3.style.setProperty("-moz-transform", t3), e3.style.setProperty("-ms-transform", t3), e3.style.setProperty("-webkit-transform", t3), e3.style.setProperty("-o-transform", t3);
	      }(e2, `scale(${t2 = t2 || 1}, ${i2 = i2 || 1})`), function(e3, t3) {
	        e3.style.setProperty("transform-origin", t3), e3.style.setProperty("-moz-transform-origin", t3), e3.style.setProperty("-ms-transform-origin", t3), e3.style.setProperty("-webkit-transform-origin", t3), e3.style.setProperty("-o-transform-origin", t3);
	      }(e2, s2);
	    }
	    function le(e2) {
	      let t2 = te("div");
	      return t2.textContent = e2, t2.innerHTML;
	    }
	    function he(e2) {
	      if (e2 && e2.length < 6) {
	        let t3 = 1, i2 = e2.split("");
	        for ("#" !== i2[0] && (t3 = 0); t3 < i2.length; t3++)
	          i2[t3] = i2[t3] + i2[t3];
	        e2 = i2.join("");
	      }
	      let t2 = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(e2);
	      return t2 ? parseInt(t2[1], 16) + ", " + parseInt(t2[2], 16) + ", " + parseInt(t2[3], 16) : null;
	    }
	    function ce(e2) {
	      return !!e2 && e2.indexOf(",") > -1;
	    }
	    function de(e2) {
	      if ("transparent" === e2.toLowerCase())
	        return 0;
	      if (!ce(e2))
	        return 1;
	      if (-1 !== e2.indexOf("#")) {
	        let t3 = he(e2);
	        if (null === t3)
	          return 1;
	        e2 = t3;
	      }
	      let t2 = e2.split(",").map(function(e3) {
	        return e3.trim();
	      });
	      return 4 === t2.length ? parseFloat((parseFloat(t2[3].replace(/[()]/g, "")) / 255).toFixed(2)) : 1;
	    }
	    function ue(e2, t2) {
	      let i2 = te("div");
	      for (i2.innerHTML = t2; i2.childNodes.length; )
	        e2.appendChild(i2.childNodes[0]);
	    }
	    function pe(e2, t2) {
	      let i2 = te("div");
	      for (i2.innerHTML = t2; i2.childNodes.length; )
	        e2.prepend(i2.childNodes[i2.childNodes.length - 1]);
	    }
	    function ge(e2, t2) {
	      return e2 ? e2.querySelector(t2) : null;
	    }
	    function me(e2, t2) {
	      var i2;
	      return e2 && e2.attributes && (null === (i2 = e2.attributes[t2]) || void 0 === i2 ? void 0 : i2.value) || "";
	    }
	    function fe(e2) {
	      let t2 = e2.parentElement;
	      return t2 ? t2.clientHeight != t2.scrollHeight ? t2 : fe(t2) : null;
	    }
	    function ve(e2, t2 = 300) {
	      let i2;
	      return function(...n2) {
	        clearTimeout(i2), i2 = setTimeout(() => e2.apply(this, n2), t2);
	      };
	    }
	    function Pe(e2, t2) {
	      let i2 = null;
	      return function(n2, ...r2) {
	        i2 || (i2 = setTimeout(function() {
	          e2.apply(n2, r2), i2 = null;
	        }, t2));
	      };
	    }
	    function Ce(e2, t2) {
	      return !!e2.responseJSON && e2.responseJSON.exceptionType === t2;
	    }
	    function ye(e2) {
	      return Ce(e2, "Telerik.Reporting.Services.Engine.InvalidClientException");
	    }
	    function Se(e2) {
	      return Ce(e2, "Telerik.Reporting.Services.Engine.InvalidParameterException");
	    }
	    function Ie(e2) {
	      return !!e2 && "internalservererror" === e2.split(" ").join("").toLowerCase();
	    }
	    function be(e2, ...t2) {
	      return e2.replace(/{(\d+)}/g, (e3, i2) => t2[i2] || "");
	    }
	    function we(e2, t2) {
	      let i2, n2;
	      if (Te(e2))
	        for (i2 = e2.length, n2 = 0; n2 < i2 && false !== t2.call(e2[n2], n2, e2[n2]); n2++)
	          ;
	      else
	        for (n2 in e2)
	          if (false === t2.call(e2[n2], n2, e2[n2]))
	            break;
	      return e2;
	    }
	    function Te(e2) {
	      if (Array.isArray(e2))
	        return true;
	      return "number" == typeof (!!e2 && "length" in e2 && e2.length);
	    }
	    function Ae(e2) {
	      return /^(\-|\+)?([0-9]+)$/.test(e2) ? Number(e2) : NaN;
	    }
	    function Re(e2) {
	      return /^(\-|\+)?([0-9]+(\.[0-9]+)?)$/.test(e2) ? Number(e2) : NaN;
	    }
	    function Ee(e2) {
	      return e2 instanceof Date ? e2 : (/Z|[\+\-]\d\d:?\d\d/i.test(e2) || (e2 += "Z"), new Date(e2));
	    }
	    e.PageMode = void 0, (K = e.PageMode || (e.PageMode = {}))[K.ContinuousScroll = 0] = "ContinuousScroll", K[K.SinglePage = 1] = "SinglePage", e.PrintMode = void 0, (X = e.PrintMode || (e.PrintMode = {}))[X.AutoSelect = 0] = "AutoSelect", X[X.ForcePDFPlugin = 1] = "ForcePDFPlugin", X[X.ForcePDFFile = 2] = "ForcePDFFile", e.ScaleMode = void 0, (Y = e.ScaleMode || (e.ScaleMode = {}))[Y.FitPageWidth = 0] = "FitPageWidth", Y[Y.FitPage = 1] = "FitPage", Y[Y.Specific = 2] = "Specific", e.ServiceType = void 0, (Q = e.ServiceType || (e.ServiceType = {}))[Q.REST = 0] = "REST", Q[Q.ReportServer = 1] = "ReportServer", e.ViewMode = void 0, (ee = e.ViewMode || (e.ViewMode = {}))[ee.Interactive = 0] = "Interactive", ee[ee.PrintPreview = 1] = "PrintPreview";
	    class Me {
	      constructor(e2, t2) {
	        this.handled = false, this.deviceInfo = e2, this.format = t2;
	      }
	    }
	    class xe extends r {
	      constructor(e2, t2, i2) {
	        super(), this.handled = false, this.body = e2.body, this.cc = e2.cc, this.format = e2.format, this.from = e2.from, this.subject = e2.subject, this.to = e2.to, this.deviceInfo = t2, this.url = i2;
	      }
	    }
	    const ke = "System.Int64", Le = "System.Double", Ne = "System.String", De = "System.DateTime", Fe = "System.Boolean";
	    var Oe = function() {
	      var e2 = {};
	      function t2(e3, t3, i3, n2) {
	        var r2 = [].concat(t3).map(function(t4) {
	          return function(e4, t5, i4) {
	            if (e4.availableValues) {
	              var n3 = false;
	              if (we(e4.availableValues, function(e5, r3) {
	                return !(n3 = i4(t5, r3.value));
	              }), !n3) {
	                if (e4.allowNull && !t5)
	                  return t5;
	                throw "Please input a valid value.";
	              }
	            }
	            return t5;
	          }(e3, i3(t4), n2);
	        });
	        if (e3.multivalue) {
	          if (null == t3 || 0 == t3.length) {
	            if (e3.allowNull)
	              return t3;
	            throw "Please input a valid value.";
	          }
	          return r2;
	        }
	        return r2[0];
	      }
	      function i2(e3, t3) {
	        return e3.allowNull && -1 != [null, "", void 0].indexOf(t3);
	      }
	      return e2[Ne] = { validate: function(e3, i3) {
	        return t2(e3, i3, function(t3) {
	          if (!t3) {
	            if (e3.allowNull)
	              return null;
	            if (e3.allowBlank)
	              return "";
	            throw "Parameter value cannot be empty.";
	          }
	          return t3;
	        }, function(e4, t3) {
	          return e4 == t3;
	        });
	      } }, e2[Le] = { validate: function(e3, n2) {
	        return t2(e3, n2, function(t3) {
	          var n3 = Re(t3);
	          if (isNaN(n3)) {
	            if (i2(e3, t3))
	              return null;
	            throw "Parameter value cannot be empty.";
	          }
	          return n3;
	        }, function(e4, t3) {
	          return Re(e4) == Re(t3);
	        });
	      } }, e2[ke] = { validate: function(e3, n2) {
	        return t2(e3, n2, function(t3) {
	          var n3 = Ae(t3);
	          if (isNaN(n3)) {
	            if (i2(e3, t3))
	              return null;
	            throw "Parameter value cannot be empty.";
	          }
	          return n3;
	        }, function(e4, t3) {
	          return Ae(e4) == Re(t3);
	        });
	      } }, e2[De] = { validate: function(e3, i3) {
	        return t2(e3, i3, function(t3) {
	          if (e3.allowNull && (null === t3 || "" === t3 || void 0 === t3))
	            return null;
	          if (!isNaN(Date.parse(t3)))
	            return e3.availableValues ? t3 : Ee(t3);
	          throw "Please input a valid date.";
	        }, function(e4, t3) {
	          return e4 = Ee(e4), t3 = Ee(t3), e4.getTime() == t3.getTime();
	        });
	      } }, e2[Fe] = { validate: function(e3, n2) {
	        return t2(e3, n2, function(t3) {
	          if (-1 != ["true", "false"].indexOf(String(t3).toLowerCase()))
	            return Boolean(t3);
	          if (i2(e3, t3))
	            return null;
	          throw "Parameter value cannot be empty.";
	        }, function(e4, t3) {
	          return Boolean(e4) == Boolean(t3);
	        });
	      } }, { validate: function(t3, i3) {
	        var n2 = e2[t3.type];
	        if (!n2)
	          throw be("Cannot validate parameter of type {type}.", t3);
	        return n2.validate(t3, i3);
	      } };
	    }();
	    function Ve(e2, t2, i2) {
	      try {
	        const n2 = e2.availableValues.find((e3) => e3.value === t2);
	        if (!n2) {
	          if (e2.allowNull && null === t2)
	            return { valueMember: null, displayMember: null };
	          throw new Error(`The available values of parameter ${i2} do not contain Value property that equals ${t2}`);
	        }
	        return { valueMember: t2, displayMember: n2.name };
	      } catch (e3) {
	        return;
	      }
	    }
	    function ze(e2, t2, i2) {
	      const n2 = [];
	      for (let r2 in t2)
	        n2.push(Ve(e2, t2[r2], i2));
	      return n2;
	    }
	    class _e {
	      constructor(e2 = "", t2 = {}) {
	        this.report = e2, this.parameters = t2;
	      }
	    }
	    class Ue extends $ {
	      constructor(e2) {
	        super(), this.resizeObserver = null, this.element = e2, this.initResizeObserver();
	      }
	      destroy() {
	        this.destroyResizeObserver();
	      }
	      initResizeObserver() {
	        this.debounceResize = ve(this.onResize.bind(this), 50), this.resizeObserver = new ResizeObserver(this.debounceResize), this.resizeObserver.observe(this.element);
	      }
	      destroyResizeObserver() {
	        this.resizeObserver && this.resizeObserver.unobserve(this.element), this.resizeObserver = this.debounceResize = null;
	      }
	      onResize(e2) {
	        e2[0].target === this.element && this.emit("resize");
	      }
	    }
	    const $e = '<div class="trv-report-page trv-skeleton-page trv-skeleton-{0}" style="{1}" data-page="{0}"><div class="trv-skeleton-wrapper" style="{2}"></div></div>';
	    class He {
	      constructor(t2, i2, n2) {
	        this.enabled = false, this.viewMode = e.ViewMode.Interactive, this.scrollInProgress = false, this.additionalTopOffset = 130, this.onClickHandler = null, this.debounceScroll = null, this.throttleScroll = null, this.oldScrollTopPosition = 0, this.lastLoadedPage = null, this.placeholder = t2, this.pageContainer = t2.querySelector(".trv-page-container"), this.pageWrapper = t2.querySelector(".trv-page-wrapper"), this.contentArea = i2, this.controller = n2, this.controller.getPageMode() === e.PageMode.ContinuousScroll && this.enable(), this.controller.on("loadedReportChange", this.disable.bind(this)).on("viewModeChanged", this.disable.bind(this)).on("scaleChanged", this.onScaleChanged.bind(this)).on("interactiveActionExecuting", this.onInteractiveActionExecuting.bind(this)).on("pageLoaded", this.onPageLoaded.bind(this));
	      }
	      onPageLoaded() {
	        this.currentPageNumber() > 0 && !this.scrollInProgress && setTimeout(() => {
	          this.loadMorePages();
	        }, 100);
	      }
	      onScaleChanged() {
	        this.enabled && setTimeout(() => {
	          this.loadMorePages(), this.keepCurrentPageInToView();
	        }, 100);
	      }
	      onInteractiveActionExecuting(e2) {
	        const t2 = e2.action.Type;
	        !this.enabled || "sorting" !== t2 && "toggleVisibility" !== t2 || this.disable();
	      }
	      getEnabled() {
	        return this.enabled;
	      }
	      enable() {
	        this.enabled = true, ie(this.placeholder, "scrollable"), this.initEvents();
	      }
	      disable() {
	        this.enabled && (this.lastLoadedPage = null, this.pageWrapper.innerHTML = "", this.enabled = false, ne(this.placeholder, "scrollable"), this.unbind());
	      }
	      renderPage(e2) {
	        let t2 = this.controller.getViewMode(), i2 = this.findPageElement(e2.pageNumber);
	        this.enabled ? (t2 === this.viewMode && i2 ? this.navigateToPage(e2, i2) : this.updatePageArea(e2), this.viewMode = this.controller.getViewMode()) : (this.enable(), t2 === this.viewMode && i2 ? (this.render(e2, true), this.pageContainer.scrollTop = 3, this.setCurrentPage(e2.pageNumber)) : this.updatePageArea(e2), this.viewMode = this.controller.getViewMode(), this.loadMorePages());
	      }
	      navigateToElement(e2, t2) {
	        this.scrollInProgress = true, this.isSkeletonScreen(null, t2) ? this.getAndRenderPage(t2).then(() => {
	          this.scrollToPage(e2, t2);
	        }) : this.scrollToPage(e2, t2);
	      }
	      updatePageDimensionsReady() {
	        this.enabled && this.currentPageNumber() > 0 && this.keepCurrentPageInToView();
	      }
	      setCurrentPage(e2) {
	        e2 !== this.currentPageNumber() && this.controller.setCurrentPageNumber(e2), this.controller.getPageCount() > 1 && (ne(this.findElement(".k-state-default"), "k-state-default"), ie(this.findPageElement(e2), "k-state-default")), this.loadNextPreviousPage(e2);
	      }
	      updatePageArea(e2) {
	        var t2;
	        let i2 = 0, n2 = e2.pageNumber;
	        this.scrollInProgress = true, n2 > 1 && this.generateSkeletonScreens(n2), this.render(e2, false), this.setCurrentPage(e2.pageNumber), i2 = n2 > 1 && (null === (t2 = this.findPageElement(n2)) || void 0 === t2 ? void 0 : t2.offsetTop) || 0, this.animateScroll(i2, 0, () => {
	          this.scrollInProgress = false;
	        });
	      }
	      navigateToPage(e2, t2) {
	        this.scrollInProgress = true;
	        let i2 = t2.offsetTop, n2 = e2.pageNumber;
	        this.isSkeletonScreen(t2, n2) ? this.getAndRenderPage(n2).then(() => {
	          this.scrollToPage(i2, n2);
	        }) : (this.updatePageContent(e2, t2), this.scrollToPage(i2, n2));
	      }
	      updatePageContent(e2, t2) {
	        this.contentArea.updatePageStyle(e2);
	        let i2 = this.contentArea.renderPageElement(e2);
	        t2.after(i2), t2.remove(), this.contentArea.setScrollablePage(i2, e2), this.controller.applySearchColors();
	      }
	      currentPageNumber() {
	        return this.controller.getCurrentPageNumber();
	      }
	      isSkeletonScreen(e2, t2) {
	        return !(!e2 && !(e2 = this.findPageElement(t2))) && re(e2, "trv-skeleton-" + t2);
	      }
	      addSkeletonScreen(e2, t2) {
	        let i2 = e2 + (t2 ? 1 : -1), n2 = this.findPageElement(i2), r2 = me(n2, "style"), s2 = me(null == n2 ? void 0 : n2.querySelector("sheet"), "style"), o2 = be($e, e2, r2, s2);
	        t2 ? pe(this.pageWrapper, o2) : ue(this.pageWrapper, o2);
	      }
	      generateSkeletonScreens(e2) {
	        var t2;
	        let i2 = "", n2 = this.findPageElement(1), r2 = me(n2, "style"), s2 = me(null == n2 ? void 0 : n2.querySelector("sheet"), "style"), o2 = null === (t2 = this.findLastElement(".trv-report-page")) || void 0 === t2 ? void 0 : t2.dataset.page, a2 = o2 ? parseInt(o2) + 1 : 1;
	        for (; a2 < e2; a2++)
	          i2 += be($e, a2, r2, s2);
	        ue(this.pageWrapper, i2);
	      }
	      loadMorePages() {
	        var e2;
	        let t2 = this.controller.getPageCount(), i2 = this.pageContainer.offsetHeight > this.pageWrapper.offsetHeight;
	        if (t2 > 1)
	          if (i2) {
	            this.scrollInProgress = true;
	            let i3 = null === (e2 = this.findLastElement(".trv-report-page")) || void 0 === e2 ? void 0 : e2.dataset.page, n2 = i3 ? parseInt(i3) + 1 : 1;
	            n2 <= t2 && this.getAndRenderPage(n2).then(() => {
	              this.loadMorePages(), this.scrollInProgress = false;
	            });
	          } else
	            this.loadVisiblePages(), this.scrollInProgress = false;
	      }
	      loadVisiblePages() {
	        this.placeholder.querySelectorAll(".trv-report-page").forEach((e2) => {
	          let t2 = e2, i2 = parseInt(t2.dataset.page || "");
	          this.scrolledInToView(t2) && this.isSkeletonScreen(t2, i2) && this.getAndRenderPage(i2);
	        });
	      }
	      scrolledInToView(e2) {
	        if (!e2)
	          return false;
	        let t2 = e2.getBoundingClientRect(), i2 = e2.closest(".trv-pages-pane");
	        if (!i2)
	          return false;
	        let n2 = i2.getBoundingClientRect(), r2 = n2.top, s2 = n2.bottom, o2 = t2.top, a2 = t2.bottom, l2 = this.additionalTopOffset + r2;
	        return o2 > 0 && o2 < s2 || a2 < s2 && a2 > l2;
	      }
	      render(e2, t2) {
	        let i2 = e2.pageNumber, n2 = this.findPageElement(i2);
	        if (!t2 && n2 && !this.isSkeletonScreen(n2, i2))
	          return;
	        (null == this.lastLoadedPage || this.lastLoadedPage.pageNumber < e2.pageNumber) && (this.lastLoadedPage = e2), this.contentArea.updatePageStyle(this.lastLoadedPage);
	        let r2 = this.contentArea.renderPageElement(e2);
	        t2 && (this.pageWrapper.innerHTML = ""), this.pageWrapper.dataset.pageNumber = i2.toString();
	        let s2 = this.findElement(".trv-skeleton-" + i2);
	        s2 ? (s2.after(r2), s2.remove()) : this.pageWrapper.append(r2), this.contentArea.setScrollablePage(r2, e2), this.controller.applySearchColors();
	      }
	      loadNextPreviousPage(e2) {
	        if (e2 < this.controller.getPageCount()) {
	          let t2 = e2 + 1, i2 = this.findPageElement(t2);
	          i2 && this.isSkeletonScreen(i2, t2) && this.getAndRenderPage(t2);
	        }
	        if (e2 > 1) {
	          let t2 = e2 - 1, i2 = this.findPageElement(t2);
	          i2 && this.isSkeletonScreen(i2, t2);
	        }
	      }
	      initEvents() {
	        this.onClickHandler = this.clickPage.bind(this), this.debounceScroll = ve(() => {
	          let e2 = this.placeholder.querySelectorAll(".trv-report-page"), t2 = Math.round(this.pageContainer.scrollTop + this.pageContainer.offsetHeight);
	          !this.scrollInProgress && e2.length && this.oldScrollTopPosition !== t2 && this.advanceCurrentPage(Array.from(e2));
	        }, 250), this.throttleScroll = Pe(() => {
	          let e2 = this.placeholder.querySelectorAll(".trv-report-page"), t2 = Math.round(this.pageContainer.scrollTop + this.pageContainer.offsetHeight);
	          this.scrollInProgress || this.oldScrollTopPosition === t2 || (this.oldScrollTopPosition > t2 ? this.scrollUp(Array.from(e2)) : this.scrollDown(Array.from(e2), t2)), this.oldScrollTopPosition = t2;
	        }, 250), this.pageContainer.addEventListener("click", this.onClickHandler), this.pageContainer.addEventListener("scroll", this.debounceScroll), this.pageContainer.addEventListener("scroll", this.throttleScroll);
	      }
	      unbind() {
	        this.pageContainer.removeEventListener("click", this.onClickHandler), this.pageContainer.removeEventListener("scroll", this.debounceScroll), this.pageContainer.removeEventListener("scroll", this.throttleScroll);
	      }
	      clickPage(e2) {
	        var t2;
	        let i2 = null === (t2 = e2.target) || void 0 === t2 ? void 0 : t2.closest(".trv-report-page");
	        if (!i2)
	          return;
	        let n2 = this.currentPageNumber(), r2 = parseInt(i2.dataset.page || "");
	        n2 !== r2 && (this.isSkeletonScreen(i2, r2) ? this.getAndRenderPage(r2).then((e3) => {
	          this.setCurrentPage(e3.pageNumber);
	        }) : this.setCurrentPage(r2));
	      }
	      advanceCurrentPage(e2) {
	        let t2 = this.findNewCurrentPage(e2);
	        if (t2) {
	          let e3 = parseInt(t2.dataset.page || "-1"), i2 = this.currentPageNumber(), n2 = this.scrolledInToView(this.findPageElement(i2));
	          i2 === e3 || n2 || (this.isSkeletonScreen(t2, e3) ? this.getAndRenderPage(e3).then((e4) => {
	            this.setCurrentPage(e4.pageNumber);
	          }) : this.setCurrentPage(e3));
	        } else
	          console.log("Page not found - ", t2);
	      }
	      findNewCurrentPage(e2) {
	        let t2 = Math.floor(e2.length / 2), i2 = this.findPageInViewPort(t2, e2);
	        return 1 === e2.length ? e2[0] : 0 === i2 ? e2[t2] : i2 < 0 && e2.length > 1 ? this.findNewCurrentPage(e2.splice(t2, Number.MAX_VALUE)) : i2 > 0 && e2.length > 1 ? this.findNewCurrentPage(e2.splice(0, t2)) : null;
	      }
	      findPageInViewPort(e2, t2) {
	        let i2 = t2[e2], n2 = i2.getBoundingClientRect(), r2 = i2.closest(".trv-pages-pane");
	        if (!r2)
	          return -1;
	        let s2 = r2.getBoundingClientRect().top, o2 = n2.top, a2 = n2.bottom, l2 = this.additionalTopOffset + s2;
	        return o2 <= l2 && l2 < a2 ? 0 : a2 < l2 ? -1 : 1;
	      }
	      scrollDown(e2, t2) {
	        if (t2 >= this.pageContainer.scrollHeight - 5) {
	          let t3 = parseInt(e2[e2.length - 1].dataset.page || "") + 1;
	          this.currentPageNumber() < t3 && t3 <= this.controller.getPageCount() && (this.addSkeletonScreen(t3, false), this.getAndRenderPage(t3));
	        } else
	          this.advanceCurrentPage(e2), this.loadVisiblePages();
	      }
	      scrollUp(e2) {
	        if (0 === this.pageContainer.scrollTop) {
	          let t2 = parseInt(e2[0].dataset.page || "") - 1;
	          this.currentPageNumber() > t2 && t2 >= 1 && (this.addSkeletonScreen(t2, true), this.getAndRenderPage(t2).then(() => {
	            this.pageContainer.scrollTop = 3;
	          }));
	        } else
	          this.advanceCurrentPage(e2), this.loadVisiblePages();
	      }
	      keepCurrentPageInToView() {
	        let e2 = this.findPageElement(this.currentPageNumber());
	        if (!e2)
	          return;
	        let t2 = e2.offsetTop, i2 = e2.offsetHeight, n2 = this.pageContainer.offsetHeight;
	        if (this.scrollInProgress = true, i2 < n2) {
	          t2 -= (n2 - i2) / 2;
	        }
	        this.animateScroll(t2, 0, () => {
	          setTimeout(() => {
	            this.scrollInProgress = false;
	          }, 100);
	        });
	      }
	      getAndRenderPage(e2) {
	        return this.controller.getPageData(e2).then((e3) => (this.render(e3, false), e3));
	      }
	      scrollToPage(e2, t2) {
	        this.animateScroll(e2, 500, () => {
	          setTimeout(() => {
	            this.setCurrentPage(t2), this.scrollInProgress = false;
	          });
	        });
	      }
	      animateScroll(e2, t2, i2) {
	        this.pageContainer.scrollTop = e2, i2.call(this);
	      }
	      findPageElement(e2) {
	        return this.findElement('[data-page="' + e2 + '"]');
	      }
	      findElement(e2) {
	        var t2;
	        return null === (t2 = this.placeholder) || void 0 === t2 ? void 0 : t2.querySelector(e2);
	      }
	      findLastElement(e2) {
	        var t2;
	        let i2 = null === (t2 = this.placeholder) || void 0 === t2 ? void 0 : t2.querySelectorAll(e2);
	        return i2 && i2.length ? i2[i2.length - 1] : null;
	      }
	    }
	    class Be {
	      constructor() {
	        this.scaleFactor = 0, this.placeholder = null, this.scrollableContainer = null, this.itemsInitialState = {}, this.xFrozenAreasBounds = {}, this.yFrozenAreasBounds = {}, this.freezeMaxZIndex = {}, this.freezeBGColor = {}, this.currentlyFrozenContainer = { vertical: {}, horizontal: {} }, this.zIndex = 1;
	      }
	      init(e2) {
	        this.reset(e2), this.attachToScrollEvent();
	      }
	      reset(e2) {
	        this.placeholder = e2, this.scrollableContainer = ge(e2, ".trv-page-container"), this.itemsInitialState = {}, this.xFrozenAreasBounds = {}, this.yFrozenAreasBounds = {}, this.currentlyFrozenContainer = { vertical: {}, horizontal: {} };
	      }
	      setScaleFactor(e2) {
	        this.scaleFactor = e2;
	      }
	      attachToScrollEvent() {
	        var e2;
	        null === (e2 = this.scrollableContainer) || void 0 === e2 || e2.addEventListener("scroll", () => {
	          if (this.scrollableContainer) {
	            let e3 = this.scrollableContainer.querySelectorAll("div[data-sticky-id]");
	            if (e3.length) {
	              const t2 = new Set(Array.from(e3).map((e4) => e4.dataset.stickyId || ""));
	              let i2 = this.scrollableContainer.scrollTop, n2 = this.scrollableContainer.scrollLeft;
	              t2.forEach((e4) => {
	                this.itemsInitialState[e4] || this.saveFreezeItemsInitialState(e4), this.updateFreezeItemsOnScroll(e4, i2, n2);
	              });
	            }
	          }
	        });
	      }
	      saveFreezeItemsInitialState(e2) {
	        var t2, i2, n2;
	        let r2 = null === (t2 = this.placeholder) || void 0 === t2 ? void 0 : t2.querySelectorAll("[data-sticky-direction][data-sticky-id='" + e2 + "']"), s2 = null === (i2 = this.placeholder) || void 0 === i2 ? void 0 : i2.querySelectorAll("[data-reporting-action][data-sticky-id='" + e2 + "']"), o2 = null, a2 = null, l2 = null, h2 = null;
	        this.itemsInitialState[e2] = {}, this.freezeBGColor[e2] = (null === (n2 = ge(this.placeholder, "[data-id='" + e2 + "']")) || void 0 === n2 ? void 0 : n2.dataset.stickyBgColor) || "", r2.forEach((t3) => {
	          var i3;
	          let n3 = t3.dataset.stickyDirection, r3 = (null === (i3 = t3.dataset.id) || void 0 === i3 ? void 0 : i3.toString()) || "", s3 = t3.offsetLeft / this.scaleFactor, c2 = t3.offsetLeft + t3.offsetWidth * this.scaleFactor, d2 = t3.offsetTop / this.scaleFactor, u2 = t3.offsetTop + t3.offsetHeight * this.scaleFactor, p2 = (e3, t4) => null === e3 || t4 < e3 ? t4 : e3, g2 = (e3, t4) => null === e3 || t4 > e3 ? t4 : e3;
	          switch (n3) {
	            case "Vertical":
	              a2 = p2(a2, d2), h2 = g2(h2, u2);
	              break;
	            case "Horizontal":
	              o2 = p2(o2, s3), l2 = g2(l2, c2);
	          }
	          this._saveFreezeItemInitialState(e2, t3, r3);
	        }), this.freezeMaxZIndex[e2] = (null == s2 ? void 0 : s2.length) ? parseInt(getComputedStyle(s2[0]).zIndex) : this.zIndex, this.yFrozenAreasBounds[e2] = (h2 || 0) - (a2 || 0), this.xFrozenAreasBounds[e2] = (l2 || 0) - (o2 || 0);
	      }
	      _saveFreezeItemInitialState(e2, t2, i2) {
	        const n2 = getComputedStyle(t2);
	        let r2 = this.hasSetBgColor(n2.backgroundColor), s2 = n2.zIndex;
	        "auto" !== s2 && (s2 = parseFloat(s2));
	        let o2 = { top: t2.offsetTop, left: t2.offsetLeft, zIndex: s2, hasBgColor: r2 };
	        this.itemsInitialState[e2][i2] = o2;
	      }
	      updateFreezeItemsOnScroll(e2, t2, i2) {
	        var n2, r2;
	        let s2 = ge(this.placeholder, "div[data-id='" + e2 + "']");
	        if (!s2)
	          return;
	        let o2 = null === (n2 = this.placeholder) || void 0 === n2 ? void 0 : n2.querySelectorAll("[data-sticky-direction*='Horizontal'][data-sticky-id='" + e2 + "']"), a2 = null === (r2 = this.placeholder) || void 0 === r2 ? void 0 : r2.querySelectorAll("[data-sticky-direction*='Vertical'][data-sticky-id='" + e2 + "']");
	        if (this.isInScrollVisibleArea(s2)) {
	          let n3 = s2.closest(".trv-report-page"), r3 = getComputedStyle(n3), l2 = parseFloat(r3.marginLeft), h2 = parseFloat(r3.paddingTop), c2 = parseFloat(r3.paddingLeft), d2 = parseFloat(r3.borderTopWidth), u2 = parseFloat(r3.borderLeftWidth), p2 = o2.length > 0, g2 = a2.length > 0, m2 = s2.offsetTop + ((null == n3 ? void 0 : n3.offsetTop) || 0) + l2 + h2 + d2, f2 = s2.offsetLeft + ((null == n3 ? void 0 : n3.offsetLeft) || 0) + c2 + u2, v2 = t2 - m2, P2 = i2 - f2;
	          g2 && v2 > 0 ? t2 <= s2.offsetHeight * this.scaleFactor + m2 - this.yFrozenAreasBounds[e2] && (this.currentlyFrozenContainer.vertical[e2] = true, this.updateUIElementsPosition(a2, "top", v2 / this.scaleFactor, e2)) : this.currentlyFrozenContainer.vertical[e2] && (delete this.currentlyFrozenContainer.vertical[e2], this.updateUIElementsPosition(a2, "top", -1, e2)), p2 && P2 > 0 ? i2 <= s2.offsetWidth * this.scaleFactor + f2 - this.xFrozenAreasBounds[e2] && (this.currentlyFrozenContainer.horizontal[e2] = true, this.updateUIElementsPosition(o2, "left", P2 / this.scaleFactor, e2)) : this.currentlyFrozenContainer.horizontal[e2] && (delete this.currentlyFrozenContainer.horizontal[e2], this.updateUIElementsPosition(o2, "left", -1, e2));
	        } else
	          (this.currentlyFrozenContainer.horizontal[e2] || this.currentlyFrozenContainer.vertical[e2]) && this.resetToDefaultPosition(e2, o2, a2);
	      }
	      resetToDefaultPosition(e2, t2, i2) {
	        this.updateUIElementsPosition(i2, "top", -1, e2), this.updateUIElementsPosition(t2, "left", -1, e2), delete this.currentlyFrozenContainer.horizontal[e2], delete this.currentlyFrozenContainer.vertical[e2];
	      }
	      updateUIElementsPosition(e2, t2, i2, n2) {
	        e2.forEach((e3) => {
	          let r2 = (e3.dataset.stickyDirection || "").indexOf(",") > 0, s2 = e3.dataset.id || "", o2 = this.itemsInitialState[n2][s2], a2 = "top" == t2 ? o2.top : o2.left, l2 = o2.zIndex, h2 = o2.hasBgColor, c2 = 1, d2 = this.freezeMaxZIndex[n2] ? this.freezeMaxZIndex[n2] : c2;
	          c2 = r2 ? "auto" !== l2 ? l2 : d2 + 2 : "auto" !== l2 ? l2 + 1 : d2;
	          let u2 = { zIndex: c2 };
	          i2 >= 0 ? a2 += i2 : u2.zIndex = l2, h2 || this.applyBgColorOnScroll(e3, r2, h2, i2 >= 0, n2), u2[t2] = a2 + "px", Object.assign(e3.style, u2);
	        });
	      }
	      applyBgColorOnScroll(e2, t2, i2, n2, r2) {
	        "IMG" !== e2.tagName && (t2 && this.isFrozen(r2) && !i2 ? e2.style.backgroundColor = this.freezeBGColor[r2] : e2.style.backgroundColor = n2 ? this.freezeBGColor[r2] : "initial");
	      }
	      hasSetBgColor(e2) {
	        return de(e2) > 0;
	      }
	      isFrozen(e2) {
	        return this.currentlyFrozenContainer.horizontal[e2] || this.currentlyFrozenContainer.vertical[e2];
	      }
	      isInScrollVisibleArea(e2) {
	        if (!this.scrollableContainer)
	          return false;
	        const t2 = e2.getBoundingClientRect(), i2 = this.scrollableContainer.getBoundingClientRect();
	        return this.isVisibleVertically(t2, this.scrollableContainer, i2) && this.isVisibleHorizontally(t2, this.scrollableContainer, i2);
	      }
	      isVisibleVertically(e2, t2, i2) {
	        let n2 = e2.width;
	        return e2.left > t2.scrollLeft - n2 && e2.left < t2.scrollLeft + n2 + i2.width;
	      }
	      isVisibleHorizontally(e2, t2, i2) {
	        let n2 = e2.height;
	        return e2.top > t2.scrollTop - n2 && e2.top < t2.scrollTop + n2 + i2.height;
	      }
	    }
	    const qe = /{(\w+?)}/g, We = "trv-initial-image-styles";
	    function je(e2, t2) {
	      let i2 = Array.isArray(t2);
	      return e2 ? e2.replace(qe, function(e3, n2) {
	        return t2[i2 ? parseInt(n2) : n2];
	      }) : "";
	    }
	    const Je = "trv-search-dialog-shaded-result", Ge = "trv-search-dialog-highlighted-result";
	    e.BasicAuth = C, e.BookmarkNode = class {
	      constructor() {
	        this.id = "", this.text = "", this.page = 0, this.items = null;
	      }
	    }, e.ConnectionConfig = b, e.ConnectionConfigNoAuth = w, e.ConnectionConfigServerCredentials = A, e.ConnectionConfigServerNoAuth = T, e.ConnectionConfigServerToken = R, e.ContentArea = class {
	      constructor(e2, t2, i2, n2 = {}) {
	        this.actions = [], this.pendingElement = null, this.documentReady = true, this.reportPageIsLoaded = false, this.navigateToPageOnDocReady = 0, this.navigateToElementOnDocReady = null, this.onClickHandler = null, this.onMouseEnterHandler = null, this.onMouseLeaveHandler = null, this.isNewReportSource = false, this.uiFreezeCoordinator = null, this.initialPageAreaImageUrl = "", this.showPageAreaImage = false, this.placeholder = e2.querySelector(".trv-pages-pane, .trv-pages-area"), this.pageContainer = e2.querySelector(".trv-page-container"), this.pageWrapper = e2.querySelector(".trv-page-wrapper"), this.parametersContainer = e2.querySelector(".trv-parameters-area"), this.notification = e2.querySelector(".trv-notification, .trv-error-pane"), this.scrollManager = new He(this.placeholder, this, t2), this.resizeService = new Ue(this.pageContainer), this.resizeService.on("resize", this.onResize.bind(this)), this.controller = t2, this.controller.on("pageReady", this.onPageReady.bind(this)).on("navigateToPage", this.navigateToPage.bind(this)).on("serverActionStarted", this.onServerActionStarted.bind(this)).on("reportSourceChanged", this.onReportSourceChanged.bind(this)).on("scaleChanged", this.updatePageDimensions.bind(this)).on("scaleModeChanged", this.updatePageDimensions.bind(this)).on("printStarted", this.onPrintStarted.bind(this)).on("printDocumentReady", this.onPrintDocumentReady.bind(this)).on("exportStarted", this.onExportStarted.bind(this)).on("exportDocumentReady", this.onExportDocumentReady.bind(this)).onAsync("beforeLoadReport", this.onBeforeLoadReport.bind(this)).on("beginLoadReport", this.onBeginLoadReport.bind(this)).on("reportLoadProgress", this.onReportLoadProgress.bind(this)).onAsync("reportLoadComplete", this.onReportLoadComplete.bind(this)).onAsync("reportAutoRunOff", this.onReportAutoRunOff.bind(this)).on("renderingStopped", this.onRenderingStopped.bind(this)).on("missingOrInvalidParameters", this.onMissingOrInvalidParameters.bind(this)).on("noReport", this.onNoReport.bind(this)).on("error", this.onError.bind(this)).on("showNotification", this.onShowNotification.bind(this)), this.messages = i2, this.enableAccessibility = n2.enableAccessibility || false, this.initialPageAreaImageUrl = n2.initialPageAreaImageUrl || "";
	      }
	      destroy() {
	        this.resizeService && this.resizeService.destroy();
	      }
	      onResize() {
	        this.shouldAutosizePage() && this.updatePageDimensions();
	      }
	      onPageReady(t2) {
	        this.controller.getPageMode() === e.PageMode.SinglePage ? (this.scrollManager.getEnabled() && this.scrollManager.disable(), this.setPageContent(t2)) : this.scrollManager.renderPage(t2), this.navigateToElement(this.pendingElement, t2.pageNumber), this.reportPageIsLoaded || (this.reportPageIsLoaded = true), this.showPageAreaImage && this.clearPageAreaImage(), this.controller.getViewMode() === e.ViewMode.Interactive && null !== this.uiFreezeCoordinator && this.uiFreezeCoordinator.init(this.placeholder), this.disablePagesArea(false);
	      }
	      onServerActionStarted() {
	        this.disablePagesArea(true), this.disableParametersArea(true), this.showNotification(this.messages.ReportViewer_LoadingReport);
	      }
	      onReportSourceChanged() {
	        this.isNewReportSource = true, this.navigateToPageOnDocReady = 0, this.navigateToElementOnDocReady = null, this.documentReady = false;
	      }
	      onNoReport() {
	        this.disablePagesArea(false), this.disableParametersArea(false), this.clearPage(), this.showNotification(this.messages.ReportViewer_NoReport);
	      }
	      onBeforeLoadReport() {
	        this.documentReady = false, this.navigateToPageOnDocReady || (this.navigateToPageOnDocReady = 1), this.clearPendingTimeoutIds(), this.clear(), this.disablePagesArea(true), this.disableParametersArea(true), this.showNotification(this.messages.ReportViewer_LoadingReport);
	      }
	      onBeginLoadReport() {
	        this.documentReady = true, this.invalidateCurrentlyLoadedPage();
	      }
	      onReportLoadProgress(e2) {
	        this.navigateWhenPageAvailable(this.navigateToPageOnDocReady, e2.pageCount), this.showNotification(je(this.messages.ReportViewer_LoadingReportPagesInProgress, [e2.pageCount]));
	      }
	      onReportLoadComplete(t2) {
	        0 === t2.pageCount ? (this.clearPage(), this.showNotification(this.messages.ReportViewer_NoPageToDisplay)) : (this.navigateOnLoadComplete(this.navigateToPageOnDocReady, t2.pageCount), this.showNotification(je(this.messages.ReportViewer_LoadedReportPagesComplete, [t2.pageCount])), this.showNotificationTimeoutId = window.setTimeout(this.hideNotification.bind(this), 2e3), this.disableParametersArea(false), this.enableInteractivity()), t2.containsFrozenContent && null === this.uiFreezeCoordinator && (this.uiFreezeCoordinator = new Be(), this.controller.getViewMode() === e.ViewMode.Interactive && this.uiFreezeCoordinator.init(this.placeholder));
	      }
	      onReportAutoRunOff() {
	        this.disableParametersArea(false), this.showNotification(this.messages.ReportViewer_AutoRunDisabled || "Please validate the report parameter values and press Preview to generate the report.");
	      }
	      onRenderingStopped() {
	        this.clear(true), this.disableParametersArea(false), this.showError(this.messages.ReportViewer_RenderingCancelled);
	      }
	      onMissingOrInvalidParameters() {
	        this.initialPageAreaImageUrl && !this.reportPageIsLoaded && (this.clearPage(), this.setPageAreaImage());
	      }
	      onShowNotification(e2) {
	        this.showNotification(e2);
	      }
	      onError(e2, t2 = true) {
	        t2 && (this.disablePagesArea(false), this.disableParametersArea(false), this.clearPage()), this.showError(e2);
	      }
	      clearPendingTimeoutIds() {
	        this.showNotificationTimeoutId && window.clearTimeout(this.showNotificationTimeoutId);
	      }
	      invalidateCurrentlyLoadedPage() {
	        let e2 = this.findPage(this.navigateToPageOnDocReady);
	        e2 && this.setPageNo(e2, -1);
	      }
	      navigateWhenPageAvailable(e2, t2) {
	        e2 && e2 <= t2 && this.navigateToPage(e2, this.navigateToElementOnDocReady);
	      }
	      navigateOnLoadComplete(e2, t2) {
	        e2 && (e2 = Math.min(e2, t2), this.navigateToPage(e2, this.navigateToElementOnDocReady));
	      }
	      clearPage() {
	        this.clear(this.isNewReportSource), this.isNewReportSource = false;
	      }
	      shouldAutosizePage() {
	        let t2 = this.controller.getScaleMode();
	        return t2 === e.ScaleMode.FitPage || t2 === e.ScaleMode.FitPageWidth;
	      }
	      onPrintStarted() {
	        this.showNotification(this.messages.ReportViewer_PreparingPrint);
	      }
	      onPrintDocumentReady() {
	        this.hideNotification();
	      }
	      onExportStarted() {
	        this.showNotification(this.messages.ReportViewer_PreparingDownload);
	      }
	      onExportDocumentReady() {
	        this.hideNotification();
	      }
	      updatePageDimensions() {
	        let e2 = this.pageContainer.querySelectorAll(".trv-report-page");
	        for (let t2 of Array.from(e2)) {
	          let e3 = t2, i2 = parseInt(e3.dataset.page || "");
	          this.setPageDimensions(e3, i2);
	        }
	        this.scrollManager.updatePageDimensionsReady();
	      }
	      clear(e2 = false) {
	        this.disableInteractivity(), this.pendingElement = null, e2 && (this.pageWrapper.innerHTML = ""), this.hideNotification();
	      }
	      findPage(t2) {
	        let i2 = this.pageContainer.querySelectorAll(".trv-report-page");
	        if (this.controller.getPageMode() === e.PageMode.SinglePage)
	          for (let e2 of Array.from(i2)) {
	            let i3 = e2;
	            if (this.pageNo(i3) === t2)
	              return i3;
	          }
	        else
	          for (let e2 of Array.from(i2)) {
	            let i3 = e2;
	            if (parseInt((null == i3 ? void 0 : i3.dataset.page) || "-1") === t2)
	              return i3;
	          }
	        return null;
	      }
	      navigateToPage(e2, t2) {
	        this.documentReady ? this.navigateToPageCore(e2, t2) : this.navigateToPageOnDocumentReady(e2, t2);
	      }
	      navigateToPageOnDocumentReady(e2, t2) {
	        this.navigateToPageOnDocReady = e2, this.navigateToElementOnDocReady = t2;
	      }
	      navigateToPageCore(e2, t2) {
	        let i2 = this.findPage(e2);
	        i2 ? (t2 && this.navigateToElement(t2, e2), this.scrollManager.getEnabled() && !t2 && this.scrollManager.navigateToElement(i2.offsetTop, e2)) : (this.pendingElement = t2, this.beginLoadPage(e2));
	      }
	      navigateToElement(e2, t2 = 0) {
	        var i2;
	        if (e2) {
	          let n2 = this.pageContainer.querySelector(`[data-${e2.type}-id='${e2.id}']`);
	          if (n2) {
	            if (this.enableAccessibility) {
	              let e4 = this.findNextFocusableElement(n2);
	              e4 && e4.focus();
	            }
	            let e3 = 0, i3 = 0;
	            for (; n2 && n2 !== this.pageContainer; ) {
	              if (re(n2, "trv-page-wrapper")) {
	                let t3 = n2.dataset.pageScale;
	                if ("string" == typeof t3) {
	                  let n3 = parseFloat(t3);
	                  e3 *= n3, i3 *= n3;
	                }
	              }
	              e3 += n2.offsetTop, i3 += n2.offsetLeft, n2 = se(n2);
	            }
	            this.scrollManager.getEnabled() && t2 ? this.scrollManager.navigateToElement(e3, t2) : (this.pageContainer.scrollTop = e3, this.pageContainer.scrollLeft = i3);
	          } else
	            this.scrollManager.getEnabled() && t2 && this.scrollManager.navigateToElement((null === (i2 = this.placeholder) || void 0 === i2 ? void 0 : i2.querySelector('[data-page="' + t2 + '"]')).offsetTop, t2);
	        }
	      }
	      findNextFocusableElement(e2) {
	        if (!e2)
	          return null;
	        let t2 = e2.tabIndex;
	        return !isNaN(t2) && t2 > -1 ? e2 : this.findNextFocusableElement(e2.nextElementSibling);
	      }
	      disablePagesArea(e2) {
	        e2 ? ie(this.placeholder, "trv-loading") : ne(this.placeholder, "trv-loading");
	      }
	      disableParametersArea(e2) {
	        var t2, i2;
	        e2 ? null === (t2 = this.parametersContainer) || void 0 === t2 || t2.setAttribute("disabled", "") : null === (i2 = this.parametersContainer) || void 0 === i2 || i2.removeAttribute("disabled");
	      }
	      showError(e2 = "") {
	        this.showNotification(e2, "error");
	      }
	      showNotification(e2 = "", t2 = "info") {
	        let i2 = this.notification.dataset.type;
	        i2 && ne(this.notification, `k-notification-${i2}`), this.notification.dataset.type = t2;
	        let n2 = this.notification.querySelector(".k-notification-content, .trv-error-message"), r2 = null == e2 ? void 0 : e2.split(/\r?\n/);
	        n2.innerHTML = r2 && r2.length ? `${r2.join("<br>")}` : "Notification message not found.", ie(this.notification, `k-notification-${t2}`), ne(this.notification, "k-hidden");
	      }
	      hideNotification() {
	        let e2 = String(this.notification.dataset.type);
	        delete this.notification.dataset.type, ne(this.notification, `k-notification-${e2}`), ie(this.notification, "k-hidden");
	      }
	      pageNo(e2) {
	        var t2;
	        return parseInt((null === (t2 = e2.parentElement) || void 0 === t2 ? void 0 : t2.dataset.pageNumber) || "0");
	      }
	      setPageNo(e2, t2) {
	        e2.parentElement && (e2.parentElement.dataset.pageNumber = t2.toString());
	      }
	      beginLoadPage(e2) {
	        this.disablePagesArea(true), window.setTimeout(this.controller.getReportPage.bind(this.controller, e2), 1), this.navigateToPageOnDocReady = 0;
	      }
	      setPageDimensions(t2, i2 = 0) {
	        let n2 = i2 ? t2 : t2.querySelector("div.trv-report-page");
	        if (!n2)
	          return;
	        let r2, s2 = n2.querySelector("div.sheet"), o2 = n2.querySelector("div.trv-skeleton-wrapper");
	        if (s2 = s2 || o2, !s2)
	          return;
	        if (n2.dataset.box)
	          r2 = JSON.parse(n2.dataset.box);
	        else {
	          let e2 = getComputedStyle(n2), i3 = getComputedStyle(t2);
	          r2 = { padLeft: oe(i3.marginLeft) + oe(e2.borderLeftWidth) + oe(e2.paddingLeft), padRight: oe(i3.marginRight) + oe(e2.borderRightWidth) + oe(e2.paddingRight), padTop: oe(i3.marginTop) + oe(e2.borderTopWidth) + oe(e2.paddingTop), padBottom: oe(i3.marginBottom) + oe(e2.borderBottomWidth) + oe(e2.paddingBottom) }, n2.dataset.box = JSON.stringify(r2);
	        }
	        let a2 = s2.offsetWidth, l2 = s2.offsetHeight;
	        if (0 === a2) {
	          const e2 = getComputedStyle(s2);
	          a2 = parseInt(e2.width), l2 = parseInt(e2.height);
	        }
	        const h2 = this.controller.getScaleMode(), c2 = l2 > a2 && h2 === e.ScaleMode.FitPageWidth ? 20 : 0, d2 = (this.pageContainer.clientWidth - c2 - r2.padLeft - r2.padRight) / a2, u2 = (this.pageContainer.clientHeight - 1 - r2.padTop - r2.padBottom) / l2;
	        let p2 = this.controller.getScale();
	        h2 === e.ScaleMode.FitPageWidth ? p2 = d2 : p2 && h2 !== e.ScaleMode.FitPage || (p2 = Math.min(d2, u2)), null !== this.uiFreezeCoordinator && this.uiFreezeCoordinator.setScaleFactor(p2), t2.dataset.pageScale = p2.toString(), n2.dataset.pageScale = p2.toString(), o2 || ae(s2, p2, p2), n2.style.height = p2 * l2 + "px", n2.style.width = p2 * a2 + "px", this.controller.setScale(p2, true);
	      }
	      enableInteractivity() {
	        this.onClickHandler = this.onClick.bind(this), this.onMouseEnterHandler = this.onMouseEnter.bind(this), this.onMouseLeaveHandler = this.onMouseLeave.bind(this), this.pageContainer.addEventListener("click", this.onClickHandler), this.pageContainer.addEventListener("mouseenter", this.onMouseEnterHandler, true), this.pageContainer.addEventListener("mouseleave", this.onMouseLeaveHandler, true);
	      }
	      disableInteractivity() {
	        this.pageContainer.removeEventListener("click", this.onClickHandler), this.pageContainer.removeEventListener("mouseenter", this.onMouseEnterHandler), this.pageContainer.removeEventListener("mouseleave", this.onMouseLeaveHandler);
	      }
	      onClick(e2) {
	        let t2 = e2.target.closest("[data-reporting-action]");
	        t2 && this.onInteractiveItemClick(t2, e2);
	      }
	      onMouseEnter(e2) {
	        let t2 = e2.target, i2 = t2.dataset;
	        i2.reportingAction && this.onInteractiveItemEnter(t2), (i2.tooltipTitle || i2.tooltipText) && this.onToolTipItemEnter(t2, e2);
	      }
	      onMouseLeave(e2) {
	        let t2 = e2.target, i2 = t2.dataset;
	        i2.reportingAction && this.onInteractiveItemLeave(t2), (i2.tooltipTitle || i2.tooltipText) && this.onToolTipItemLeave(t2);
	      }
	      onInteractiveItemClick(e2, t2) {
	        var i2;
	        let n2 = (null === (i2 = e2.dataset.reportingAction) || void 0 === i2 ? void 0 : i2.toString()) || "", r2 = this.getAction(n2);
	        r2 && (this.navigateToPageOnDocReady = this.getNavigateToPageOnDocReady(t2, r2.Type), this.controller.executeReportAction(new s(e2, r2))), t2.stopPropagation();
	      }
	      onInteractiveItemEnter(e2) {
	        var t2;
	        let i2 = (null === (t2 = e2.dataset.reportingAction) || void 0 === t2 ? void 0 : t2.toString()) || "", n2 = this.getAction(i2);
	        n2 && this.controller.reportActionEnter(new s(e2, n2));
	      }
	      onInteractiveItemLeave(e2) {
	        var t2;
	        let i2 = (null === (t2 = e2.dataset.reportingAction) || void 0 === t2 ? void 0 : t2.toString()) || "", n2 = this.getAction(i2);
	        n2 && this.controller.reportActionLeave(new s(e2, n2));
	      }
	      onToolTipItemEnter(e2, t2) {
	        let i2 = e2.dataset.tooltipTitle, n2 = e2.dataset.tooltipText;
	        (i2 || n2) && this.controller.reportTooltipOpening(new p(e2, n2 || "", i2 || "", t2));
	      }
	      onToolTipItemLeave(e2) {
	        this.controller.reportTooltipClosing(new p(e2, "", "", null));
	      }
	      getNavigateToPageOnDocReady(e2, t2) {
	        var i2;
	        return !this.scrollManager.getEnabled() || "sorting" !== t2 && "toggleVisibility" !== t2 ? this.controller.getCurrentPageNumber() : (null === (i2 = e2.target.closest(".trv-report-page")) || void 0 === i2 ? void 0 : i2.dataset.page) || this.controller.getCurrentPageNumber();
	      }
	      getAction(e2) {
	        if (this.actions) {
	          let t2 = this.actions.find((t3) => t3.Id === e2);
	          if (t2)
	            return t2;
	        }
	        return null;
	      }
	      updatePageStyle(e2) {
	        var t2;
	        let i2 = "trv-" + this.controller.getClientId() + "-styles";
	        null === (t2 = document.getElementById(i2)) || void 0 === t2 || t2.remove();
	        let n2 = te("style", i2);
	        n2.innerHTML = e2.pageStyles, document.head.appendChild(n2);
	      }
	      setPageContent(e2) {
	        this.actions = e2.pageActions, this.updatePageStyle(e2);
	        let t2 = this.renderPageElement(e2);
	        this.pageWrapper.dataset.pageNumber = e2.pageNumber.toString(), this.pageWrapper.innerHTML = "", this.pageWrapper.append(t2), this.controller.setCurrentPageNumber(e2.pageNumber), this.controller.applySearchColors(), this.applyPlaceholderViewModeClass(), this.setPageDimensions(this.pageWrapper), this.pageContainer.scrollTop = 0, this.pageContainer.scrollLeft = 0;
	      }
	      setScrollablePage(e2, t2) {
	        this.actions && this.actions.length ? this.actions = this.actions.concat(t2.pageActions) : this.actions = t2.pageActions, this.applyPlaceholderViewModeClass(), this.setPageDimensions(e2, t2.pageNumber);
	      }
	      renderPageElement(e2) {
	        let t2 = te("div");
	        t2.innerHTML = e2.pageContent;
	        let i2 = t2.querySelector("div.sheet");
	        i2.style.margin = "0";
	        let n2 = te("div", "", "trv-report-page");
	        return n2.dataset.page = e2.pageNumber.toString(), n2.append(i2), n2.append(te("div", "", "k-overlay trv-overlay trv-page-overlay")), n2;
	      }
	      applyPlaceholderViewModeClass() {
	        this.controller.getViewMode() === e.ViewMode.Interactive ? (ne(this.placeholder, "printpreview"), ie(this.placeholder, "interactive")) : (ne(this.placeholder, "interactive"), ie(this.placeholder, "printpreview"));
	      }
	      setPageAreaImage() {
	        this.clearPageAreaImage();
	        let e2 = te("style", We);
	        e2.innerHTML = je('.trv-page-container {background: #ffffff url("{0}") no-repeat center 50px}', [this.initialPageAreaImageUrl]), document.head.appendChild(e2), this.showPageAreaImage = true;
	      }
	      clearPageAreaImage() {
	        var e2;
	        null === (e2 = document.getElementById(We)) || void 0 === e2 || e2.remove();
	      }
	    }, e.CurrentPageChangedEventArgs = u, e.DeviceInfo = n, e.DocumentInfo = class {
	      constructor() {
	        this.documentReady = false, this.documentMapAvailable = false, this.containsFrozenContent = false, this.pageCount = 0, this.documentMapNodes = [], this.bookmarkNodes = [], this.renderingExtensions = [], this.autoRunEnabled = true;
	      }
	    }, e.DocumentMapNode = class {
	      constructor() {
	        this.id = "", this.isExpanded = false, this.label = "", this.text = "", this.page = 0, this.items = [];
	      }
	    }, e.EmailInfo = r, e.ExportDocumentReadyEventArgs = h, e.ExportStartEventArgs = a, e.ExportStartedEventArgs = l, e.KeepClientAliveSentinel = Z, e.NoAuth = P, e.PageAction = class {
	      constructor() {
	        this.Id = "", this.ReportItemName = "", this.Type = "", this.Value = {};
	      }
	    }, e.PageActionEventArgs = s, e.PageInfo = class {
	      constructor() {
	        this.pageNumber = 0, this.pageReady = false, this.pageStyles = "", this.pageContent = "", this.pageActions = [];
	      }
	    }, e.PageTargetElement = o, e.ParameterInfo = class {
	      constructor() {
	        this.name = "", this.type = "", this.text = "", this.multivalue = false, this.allowNull = false, this.allowBlank = false, this.isVisible = false, this.autoRefresh = false, this.hasChildParameters = false, this.childParameters = [], this.availableValues = [], this.value = "", this.id = "", this.label = "";
	      }
	    }, e.ParameterValidators = Oe, e.ParameterValue = class {
	      constructor() {
	        this.name = "", this.value = null;
	      }
	    }, e.PersonalTokenAuth = y, e.PrintDocumentReadyEventArgs = d, e.PrintStartedEventArgs = c, e.RenderingExtension = class {
	      constructor() {
	        this.name = "", this.localizedName = "";
	      }
	    }, e.ReportController = class extends $ {
	      constructor(e2, t2) {
	        super(), this.configurationInfo = null, this.keepClientAliveSentinel = null, this.registerClientPromise = null, this.registerInstancePromise = null, this.documentFormatsPromise = null, this.clientId = "", this.reportInstanceId = "", this.documentId = "", this.threadId = "", this.parameterValues = {}, this.bookmarkNodes = [], this.renderingExtensions = null, this.pageCount = 0, this.currentPageNumber = 0, this.clientHasExpired = false, this.cancelLoad = false, this.searchInitiated = false, this.aiPromptInitiated = false, this.contentTabIndex = 0, this.respectAutoRun = true, this.processedParameterValues = {}, this.options = t2, t2.reportSource && this.setParameters(t2.reportSource.parameters), this.printManager = new J(), this.serviceClient = e2, t2.authenticationToken && this.serviceClient.setAccessToken(t2.authenticationToken);
	      }
	      get autoRunEnabled() {
	        var e2 = !this.parameterValues || !("trv_AutoRun" in this.parameterValues) || this.parameterValues.trv_AutoRun;
	        return !this.respectAutoRun || e2;
	      }
	      destroy() {
	        this.printManager && this.printManager.destroy();
	      }
	      setOptions(e2) {
	        this.clearReportState(), this.clearClientId(), this.options = e2, this.options.reportSource && this.setParameters(this.options.reportSource.parameters);
	      }
	      setAuthenticationToken(e2) {
	        this.serviceClient.setAccessToken(e2);
	      }
	      init() {
	        this.refreshReport(true, "");
	      }
	      getServiceVersion() {
	        return this.serviceClient.getServiceVersion();
	      }
	      getServiceConfiguration() {
	        return this.serviceClient.getServiceConfiguration();
	      }
	      stopRendering() {
	        this.serviceClient.deleteReportDocument(this.clientId, this.reportInstanceId, this.documentId).catch((e2) => {
	          this.handleRequestError(e2);
	        }).then(() => {
	          this.cancelLoad = true, this.resetPageNumbers(), this.emit("renderingStopped");
	        });
	      }
	      refreshReport(e2, t2, i2 = false, n2 = false) {
	        if (n2 || this.emit("loadedReportChange"), this.cancelLoad = false, this.respectAutoRun = i2 || this.respectAutoRun, this.clientHasExpired && (this.clientHasExpired = false, this.clearClientId()), "" == this.getReport())
	          return void this.emit("noReport");
	        let r2 = this.loadParameters();
	        r2.then((i3) => {
	          this.emit("parametersLoaded", i3);
	          this.processParameters(i3) || n2 || (this.autoRunEnabled ? this.refreshReportCore(e2, t2) : this.emitAsync("reportAutoRunOff"));
	        }), this.emit("reloadParameters", r2);
	      }
	      processParameters(e2) {
	        let t2 = {}, i2 = [], n2 = false;
	        for (let r2 of e2)
	          try {
	            let e3 = Oe.validate(r2, r2.value);
	            t2[r2.id] = e3;
	          } catch (e3) {
	            n2 = true, i2.push(r2);
	          }
	        return n2 ? this.missingOrInvalidParameters(i2) : this.setParameters(t2), n2;
	      }
	      allParametersAutoRefresh(e2) {
	        if (!e2 || !e2.length)
	          return true;
	        return Array.from(e2).every((e3) => !e3.isVisible || e3.autoRefresh);
	      }
	      allParametersValidForAutoRefresh(e2, t2) {
	        let i2 = true;
	        for (let n2 = e2.length - 1; i2 && n2 >= 0; n2--) {
	          let r2 = e2[n2];
	          i2 = r2.id in t2 && (Boolean(r2.autoRefresh) || !r2.isVisible);
	        }
	        return i2;
	      }
	      hasInvalidParameter(e2) {
	        for (const t2 of e2)
	          try {
	            Oe.validate(t2, t2.value);
	          } catch (e3) {
	            return true;
	          }
	        return false;
	      }
	      refreshReportCore(e2, t2 = "", i2 = "") {
	        this.loadReport(e2, t2, i2);
	      }
	      navigateToPage(e2, t2) {
	        this.emit("navigateToPage", e2, t2);
	      }
	      getReportDocumentId() {
	        return this.documentId;
	      }
	      setReportDocumentId(e2) {
	        this.documentId = e2;
	      }
	      getReportPage(e2) {
	        this.getPage(e2).then(this.onPageReady.bind(this));
	      }
	      getPageData(e2) {
	        return this.getPage(e2);
	      }
	      setParameters(e2) {
	        this.parameterValues = e2;
	      }
	      setParameter(e2, t2) {
	        this.parameterValues[e2] = t2;
	      }
	      getReportSource() {
	        var e2;
	        return new _e(null === (e2 = this.options.reportSource) || void 0 === e2 ? void 0 : e2.report, this.parameterValues);
	      }
	      setReportSource(e2) {
	        this.options.reportSource = e2, this.setParameters(this.options.reportSource.parameters), this.emit("reportSourceChanged", e2);
	      }
	      previewReport() {
	        this.emit("loadedReportChange"), this.respectAutoRun = false, this.refreshReportCore(false);
	      }
	      getClientId() {
	        return this.clientId;
	      }
	      getScale() {
	        return this.options.scale;
	      }
	      setScale(e2, t2 = false) {
	        e2 !== this.options.scale && (this.options.scale = e2, t2 || this.emit("scaleChanged", e2));
	      }
	      getScaleMode() {
	        return this.options.scaleMode;
	      }
	      setScaleMode(e2) {
	        e2 !== this.options.scaleMode && (this.options.scaleMode = e2, this.emit("scaleModeChanged", e2));
	      }
	      getViewMode() {
	        return this.options.viewMode;
	      }
	      setViewMode(e2) {
	        this.options.viewMode !== e2 && (this.options.viewMode = e2, this.options.reportSource && this.emit("viewModeChanged", e2));
	      }
	      getPageMode() {
	        return this.options.pageMode;
	      }
	      setPageMode(e2) {
	        this.options.pageMode !== e2 && (this.options.pageMode = e2, this.options.reportSource && this.emit("pageModeChanged", e2));
	      }
	      getPrintMode() {
	        return this.options.printMode;
	      }
	      setPrintMode(e2) {
	        this.options.printMode !== e2 && (this.options.printMode = e2, this.emit("printModeChanged", e2));
	      }
	      getCurrentPageNumber() {
	        return this.currentPageNumber;
	      }
	      setCurrentPageNumber(e2) {
	        this.currentPageNumber !== e2 && (this.currentPageNumber = e2, this.emit("currentPageChanged", new u(e2, this.documentId)));
	      }
	      getPageCount() {
	        return this.pageCount;
	      }
	      applySearchColors() {
	        this.emit("applySearchColors");
	      }
	      executeReportAction(e2) {
	        let t2 = e2.action;
	        window.setTimeout(() => {
	          if (this.emit("interactiveActionExecuting", e2), !e2.cancel)
	            if ("navigateToReport" === t2.Type) {
	              this.emit("serverActionStarted");
	              let e3 = t2.Value, i2 = this.fixDataContractJsonSerializer(e3.ParameterValues);
	              this.respectAutoRun = true, this.emit("navigateToReport", new _e(e3.Report, i2));
	            } else if ("navigateToBookmark" === t2.Type) {
	              let e3 = this.getPageForBookmark(this.bookmarkNodes, t2.Value), i2 = new o(t2.Value, "bookmark");
	              this.navigateToPage(e3, i2);
	            } else if ("sorting" === t2.Type)
	              this.execServerAction(t2.Id);
	            else if ("toggleVisibility" === t2.Type)
	              this.execServerAction(t2.Id);
	            else if ("navigateToUrl" === t2.Type) {
	              let e3 = t2.Value;
	              window.open(e3.Url, e3.Target);
	            } else
	              t2.Type;
	        }, 0);
	      }
	      reportActionEnter(e2) {
	        this.emit("interactiveActionEnter", e2);
	      }
	      reportActionLeave(e2) {
	        this.emit("interactiveActionLeave", e2);
	      }
	      reportTooltipOpening(e2) {
	        this.emit("toolTipOpening", e2);
	      }
	      reportTooltipClosing(e2) {
	        this.emit("toolTipClosing", e2);
	      }
	      printReport() {
	        return i(this, void 0, void 0, function* () {
	          let e2 = this.createDeviceInfo();
	          e2.ImmediatePrint = true;
	          let t2 = new c(e2);
	          this.emit("printStarted", t2), t2.handled || (this.setUIState("PrintInProgress", true), this.exportAsync("PDF", e2).then((e3) => {
	            let t3 = this.serviceClient.getDocumentUrl(this.clientId, this.reportInstanceId, e3);
	            t3 += `?${"response-content-disposition=" + (this.getCanUsePlugin() ? "inline" : "attachment")}`;
	            let i2 = new d(t3);
	            this.emit("printDocumentReady", i2), this.setUIState("PrintInProgress", false), i2.handled || this.printManager.print(t3);
	          }));
	        });
	      }
	      exportReport(e2) {
	        return i(this, void 0, void 0, function* () {
	          let t2 = this.createDeviceInfo(), n2 = new a(t2, e2);
	          if (yield this.emitAsync("exportStart", n2), !n2.isCancelled) {
	            let r2 = new l(t2, e2);
	            if (this.emit("exportStarted", r2), r2.handled)
	              return;
	            this.setUIState("ExportInProgress", true), this.exportAsync(e2, n2.deviceInfo).then((t3) => i(this, void 0, void 0, function* () {
	              let i2 = this.serviceClient.getDocumentUrl(this.clientId, this.reportInstanceId, t3);
	              i2 += "?response-content-disposition=attachment";
	              let n3 = new h(i2, e2, "_self");
	              yield this.emitAsync("exportEnd", n3), this.emit("exportDocumentReady", n3), this.setUIState("ExportInProgress", false), n3.handled || window.open(i2, n3.windowOpenTarget);
	            }));
	          }
	        });
	      }
	      sendReport(e2) {
	        let t2 = this.createDeviceInfo(), i2 = new Me(t2, e2.format);
	        this.emit("sendEmailStarted", i2), i2.handled || this.exportAsync(e2.format, t2).then((i3) => {
	          let n2 = this.serviceClient.getDocumentUrl(this.clientId, this.reportInstanceId, i3);
	          n2 += "?response-content-disposition=attachment";
	          let r2 = new xe(e2, t2, n2);
	          this.emit("sendEmailDocumentReady", r2), r2.handled || this.sendDocumentAsync(i3, e2);
	        });
	      }
	      getSearchResults(e2) {
	        return this.serviceClient.getSearchResults(this.clientId, this.reportInstanceId, this.documentId, e2).catch(this.handleSearchRequestError.bind(this)).then((e3) => e3 || []);
	      }
	      createAIThread() {
	        return this.serviceClient.createAIThread(this.clientId, this.reportInstanceId, this.getReport(), this.parameterValues).catch((e2) => this.raiseError(e2.exceptionMessage, false)).then((e2) => {
	          if (this.threadId = (null == e2 ? void 0 : e2.threadId) || "", e2.consentMessage && (e2.consentMessage = t.sanitize(e2.consentMessage, { USE_PROFILES: { html: true } }), t.removed && t.removed.length > 0 && console.warn("The AI-Powered Insights consent message was sanitized as it contained potentially malicious HTML elements or attributes.")), e2.predefinedPrompts)
	            for (let i2 = 0; i2 < e2.predefinedPrompts.length; i2++)
	              e2.predefinedPrompts[i2] = t.sanitize(e2.predefinedPrompts[i2], { USE_PROFILES: { html: true } }), t.removed && t.removed.length > 0 && console.warn("An AI-Powered Insights predefined prompt was sanitized as it contained potentially malicious HTML elements or attributes.");
	          return e2;
	        });
	      }
	      getAIResponse(e2) {
	        return this.serviceClient.getAIResponse(this.clientId, this.reportInstanceId, this.documentId, this.threadId, e2).then((e3) => e3 || "");
	      }
	      exportAsync(e2, t2) {
	        return this.initializeClient().then(this.registerInstance.bind(this)).then(() => this.registerDocumentAsync(e2, t2, false, this.documentId)).then((e3) => i(this, void 0, void 0, function* () {
	          return yield this.getDocumentInfo(false, e3).catch((e4) => this.raiseError(e4)), e3;
	        }));
	      }
	      sendDocumentAsync(e2, t2) {
	        return this.serviceClient.sendDocument(this.clientId, this.reportInstanceId, e2, t2).catch((e3) => {
	          this.handleRequestError(e3, be(this.options.messages.ReportViewer_ErrorSendingDocument, le(this.getReport())));
	        });
	      }
	      loadParameters(e2 = void 0) {
	        return this.initializeClient().then(this.getParameters.bind(this, e2));
	      }
	      loadReport(e2, t2, i2) {
	        this.clearReportState();
	        const n2 = this.getFormat(), r2 = this.createPreviewDeviceInfo();
	        this.emitAsync("beforeLoadReport", { deviceInfo: r2 }).then(() => this.initializeClient()).then(this.registerInstance.bind(this)).then(() => this.registerDocumentAsync(n2, r2, e2, t2, i2)).then((e3) => (this.documentId = e3, this.getDocumentInfo(true, this.documentId))).then(this.onReportLoadComplete.bind(this)).catch((e3) => this.raiseError(e3));
	      }
	      initializeAndStartSentinel() {
	        this.options.keepClientAlive && this.clientId && this.serviceClient.getClientsSessionTimeoutSeconds().then((e2) => {
	          this.keepClientAliveSentinel = new Z(this.serviceClient, this.clientId, e2), this.keepClientAliveSentinel.start();
	        });
	      }
	      stopSentinel() {
	        null != this.keepClientAliveSentinel && this.keepClientAliveSentinel.stop();
	      }
	      setClientId(e2) {
	        this.clientId = e2 || "", this.stopSentinel(), this.emit("clientIdChanged", this.clientId), this.initializeAndStartSentinel();
	      }
	      clearClientId() {
	        this.clientId = "", this.registerClientPromise = null, this.stopSentinel(), this.keepClientAliveSentinel = null, this.emit("clientIdChanged", this.clientId);
	      }
	      setReportInstanceId(e2) {
	        this.reportInstanceId = e2;
	      }
	      clearReportInstanceId() {
	        this.reportInstanceId = "", this.registerInstancePromise = null;
	      }
	      clearReportState() {
	        this.cancelLoad = false, this.documentId = "", this.resetPageNumbers(), this.clearReportInstanceId();
	      }
	      resetPageNumbers() {
	        this.pageCount = 0, this.currentPageNumber = 0;
	      }
	      handleSearchRequestError(e2) {
	        if (!Ce(e2, "System.ArgumentException"))
	          throw this.handleRequestError(e2, "", true), null;
	        this.throwPromiseError(e2);
	      }
	      throwPromiseError(e2) {
	        throw e2.exceptionMessage ? e2.exceptionMessage : this.options.messages.ReportViewer_PromisesChainStopError;
	      }
	      handleRequestError(e2, t2 = "", i2 = false) {
	        ye(e2) && this.onClientExpired();
	        let n2 = Ie(e2.error) ? "" : e2.error, r2 = this.formatRequestError(e2, n2, t2);
	        this.raiseError(r2), i2 || this.throwPromiseError(e2);
	      }
	      formatRequestError(e2, t2, i2) {
	        let n2 = e2.responseJSON, r2 = "";
	        if (n2) {
	          if (401 == n2.status || 403 == n2.status)
	            return this.options.messages.ReportViewer_ErrorUnauthorizedOrForbidden || "You don't have permission to access this report document.";
	          if (Se(e2))
	            return this.options.messages.ReportViewer_MissingOrInvalidParameter;
	          r2 = le(n2.message);
	          let t3 = le(n2.exceptionMessage || n2.ExceptionMessage || n2.error_description);
	          t3 && (r2 ? r2 += " " + t3 : r2 = t3);
	        } else
	          r2 = le(e2.responseText);
	        return (i2 || t2) && (r2 && (r2 = " " + r2), r2 = le(i2 || t2) + r2), ye(e2) && (r2 += "<br />" + this.options.messages.ReportViewer_ClientExpired), r2;
	      }
	      raiseError(e2, t2 = true) {
	        this.emit("error", e2, t2);
	      }
	      showNotification(e2) {
	        this.emit("showNotification", e2);
	      }
	      onClientExpired() {
	        this.clientHasExpired = true, this.emit("clientExpired");
	      }
	      initializeClient() {
	        return this.registerClientPromise || (this.registerClientPromise = this.serviceClient.registerClient().catch((e2) => {
	          const t2 = be(this.options.messages.ReportViewer_ErrorServiceUrl, [this.serviceClient.getServiceUrl()]);
	          this.handleRequestError(e2, t2);
	        }).then(this.setClientId.bind(this)).catch(this.clearClientId.bind(this))), this.registerClientPromise;
	      }
	      registerInstance() {
	        return this.registerInstancePromise || (this.registerInstancePromise = this.serviceClient.createReportInstance(this.clientId, this.getReport(), this.parameterValues).then(this.setReportInstanceId.bind(this)).catch(this.clearReportInstanceId.bind(this))), this.registerInstancePromise;
	      }
	      registerDocumentAsync(e2, t2, n2) {
	        return i(this, arguments, void 0, function* (e3, t3, i2, n3 = "", r2 = "") {
	          return (yield this.serviceClient.createReportDocument(this.clientId, this.reportInstanceId, e3, t3, !i2, n3, r2).catch((t4) => {
	            this.handleRequestError(t4, be(this.options.messages.ReportViewer_ErrorCreatingReportDocument, le(this.getReport()), le(e3)));
	          })) || "";
	        });
	      }
	      getFormat() {
	        return this.options.viewMode === e.ViewMode.PrintPreview ? "HTML5" : "HTML5Interactive";
	      }
	      getCanUsePlugin() {
	        switch (this.options.printMode) {
	          case e.PrintMode.ForcePDFFile:
	            return false;
	          case e.PrintMode.ForcePDFPlugin:
	            return true;
	          default:
	            return this.printManager.getDirectPrintState();
	        }
	      }
	      createPreviewDeviceInfo() {
	        let e2 = this.createDeviceInfo();
	        return e2.ContentOnly = true, e2.UseSVG = true, e2;
	      }
	      createDeviceInfo() {
	        let e2 = new n();
	        this.options.enableAccessibility && (e2.enableAccessibility = true, e2.contentTabIndex = this.getContentTabIndex());
	        let t2 = this.getSearchInitiated(), i2 = this.options.searchMetadataOnDemand;
	        return e2.enableSearch = !i2 || t2, e2;
	      }
	      getParameters(e2) {
	        return this.serviceClient.getParameters(this.clientId, this.getReport(), e2 || this.parameterValues).catch((e3) => {
	          this.handleRequestError(e3, this.options.messages.ReportViewer_UnableToGetReportParameters);
	        });
	      }
	      getReportParameters() {
	        if (!this.parameterValues)
	          return {};
	        const e2 = {}, t2 = this.getProcessedParameterValues();
	        for (let i2 in t2) {
	          const n2 = t2[i2], r2 = this.parameterValues[i2];
	          n2 && n2.availableValues ? n2.multivalue ? e2[i2] = ze(n2, r2, i2) : e2[i2] = Ve(n2, r2, i2) : e2[i2] = r2;
	        }
	        return e2;
	      }
	      setProcessedParameterValues(e2) {
	        this.processedParameterValues = e2;
	      }
	      getProcessedParameterValues() {
	        return this.processedParameterValues;
	      }
	      getDocumentInfo(t2, n2) {
	        return t2 && this.emit("beginLoadReport"), new Promise((r2, s2) => {
	          let o2 = () => i(this, void 0, void 0, function* () {
	            this.cancelLoad ? s2(this.options.messages.ReportViewer_RenderingCancelled) : (yield this.registerInstancePromise, this.serviceClient.getDocumentInfo(this.clientId, this.reportInstanceId, n2).then((i2) => {
	              i2 && i2.documentReady ? r2(i2) : (t2 && (this.getPageMode() === e.PageMode.ContinuousScroll && i2.pageCount > this.pageCount && this.emit("pageLoaded"), this.pageCount = i2.pageCount, this.emit("reportLoadProgress", i2)), window.setTimeout(o2, 500));
	            }).catch((e2) => {
	              "InvalidDocumentException" !== e2.responseJSON.exceptionType ? this.handleRequestError(e2, "", true) : console.warn("getDocumentInfo failed or was canceled by the user: " + e2.exceptionMessage);
	            }));
	          });
	          o2();
	        });
	      }
	      getPage(e2) {
	        return new Promise((t2) => {
	          let i2 = () => {
	            this.serviceClient.getPage(this.clientId, this.reportInstanceId, this.documentId, e2).then((e3) => {
	              e3.pageReady ? t2(e3) : window.setTimeout(i2, 500);
	            }).catch((e3) => this.handleRequestError(e3));
	          };
	          i2();
	        });
	      }
	      getPageForBookmark(e2, t2) {
	        if (e2)
	          for (let i2 = 0, n2 = e2.length; i2 < n2; i2++) {
	            let n3 = e2[i2];
	            if (n3.id === t2)
	              return n3.page;
	            let r2 = this.getPageForBookmark(n3.items, t2);
	            if (r2)
	              return r2;
	          }
	        return 0;
	      }
	      fixDataContractJsonSerializer(e2) {
	        let t2 = {};
	        return Array.isArray(e2) && e2.forEach(function(e3) {
	          t2[e3.Key] = e3.Value;
	        }), t2;
	      }
	      execServerAction(e2) {
	        this.emit("serverActionStarted"), this.refreshReportCore(false, this.documentId, e2);
	      }
	      onReportLoadComplete(e2) {
	        this.bookmarkNodes = e2.bookmarkNodes, this.pageCount = e2.pageCount, this.renderingExtensions = e2.renderingExtensions, this.emitAsync("reportLoadComplete", e2);
	      }
	      onPageReady(e2) {
	        this.emit("pageReady", e2);
	      }
	      getReport() {
	        var e2;
	        return (null === (e2 = this.options.reportSource) || void 0 === e2 ? void 0 : e2.report) || "";
	      }
	      getDocumentFormats() {
	        return this.renderingExtensions ? Promise.resolve(this.renderingExtensions) : (this.documentFormatsPromise || (this.documentFormatsPromise = this.serviceClient.getDocumentFormats().catch((e2) => {
	          this.handleRequestError(e2);
	        })), this.documentFormatsPromise);
	      }
	      setUIState(e2, t2) {
	        this.emit("setUIState", { operationName: e2, inProgress: t2 });
	      }
	      missingOrInvalidParameters(e2) {
	        let t2 = this.options.messages.ReportViewer_MissingOrInvalidParameter;
	        null == e2 || e2.forEach((e3) => {
	          t2 += `\r
${e3.text} (${e3.id})`;
	        }), this.raiseError(t2), this.emit("missingOrInvalidParameters");
	      }
	      handleParametersNotification(e2) {
	        e2 ? this.showNotification(this.options.messages.ReportViewer_TryReportPreview || "The report may now be previewed.") : this.missingOrInvalidParameters(null);
	      }
	      setSearchInitiated(e2) {
	        this.searchInitiated = e2;
	      }
	      getSearchInitiated() {
	        return this.searchInitiated;
	      }
	      setAiPromptInitiated(e2) {
	        this.aiPromptInitiated = e2;
	      }
	      getAiPromptInitiated() {
	        return this.aiPromptInitiated;
	      }
	      setContentTabIndex(e2) {
	        this.contentTabIndex = e2;
	      }
	      getContentTabIndex() {
	        return this.contentTabIndex;
	      }
	      disposeSentinel() {
	        this.stopSentinel(), this.keepClientAliveSentinel = null;
	      }
	      getConfigurationInfo() {
	        return this.configurationInfo;
	      }
	      setConfigurationInfo(e2) {
	        this.configurationInfo = e2;
	      }
	      isAiInsightsEnabled() {
	        var e2, t2;
	        return null !== (t2 = null === (e2 = this.configurationInfo.options.find((e3) => "ai-insights" === e3.name)) || void 0 === e2 ? void 0 : e2.isAvailable) && void 0 !== t2 && t2;
	      }
	      shouldShowLicenseBanner() {
	        var e2;
	        const t2 = null === (e2 = this.configurationInfo) || void 0 === e2 ? void 0 : e2.license;
	        return !(null == t2 ? void 0 : t2.isValid) && "true" !== this.loadFromSessionStorage("hideBanner");
	      }
	      shouldShowLicenseOverlay() {
	        var e2, t2;
	        return !(null === (t2 = null === (e2 = this.configurationInfo) || void 0 === e2 ? void 0 : e2.license) || void 0 === t2 ? void 0 : t2.isValid);
	      }
	      saveToSessionStorage(e2, t2) {
	        sessionStorage.setItem(e2, t2);
	      }
	      loadFromSessionStorage(e2) {
	        return sessionStorage.getItem(e2);
	      }
	      removeFromSessionStorage(e2) {
	        sessionStorage.removeItem(e2);
	      }
	      clearSessionStorage() {
	        sessionStorage.clear();
	      }
	    }, e.ReportControllerOptions = class {
	      constructor(e2, t2, i2, n2, r2, s2, o2, a2, l2, h2 = false, c2 = false) {
	        this.keepClientAlive = false, this.keepClientAlive = e2, this.authenticationToken = t2, this.reportSource = i2, this.printMode = n2, this.pageMode = r2, this.viewMode = s2, this.scaleMode = o2, this.scale = a2, this.messages = l2, this.enableAccessibility = h2, this.searchMetadataOnDemand = c2;
	      }
	    }, e.ReportServerSettings = class {
	      constructor(e2, t2, i2) {
	        this.url = e2, this.username = t2, this.password = i2;
	      }
	    }, e.ReportServerTokenSettings = class {
	      constructor(e2, t2) {
	        this.url = e2, this.getPersonalAccessToken = t2;
	      }
	    }, e.ReportSourceOptions = _e, e.RequestError = g, e.SearchInfo = class {
	      constructor() {
	        this.searchToken = "", this.matchCase = false, this.matchWholeWord = false, this.useRegularExpressions = false;
	      }
	    }, e.SearchManager = class extends $ {
	      constructor(e2, t2) {
	        super(), this.searchResults = [], this.pendingHighlightItem = null, this.highlightedElements = [], this.currentHighlightedElement = null, this.isActive = false, this.controller = t2, this.pageContainer = ge(e2, ".trv-page-container"), this.controller.on("applySearchColors", this.applySearchColors.bind(this)).on("pageReady", this.applySearchColors.bind(this));
	      }
	      search(e2) {
	        this.isActive = true, this.clearColoredItems(), this.searchResults = [], e2.searchToken && "" !== e2.searchToken ? this.controller.getSearchResults(e2).then(this.onSearchComplete.bind(this)) : this.onSearchComplete([]);
	      }
	      closeSearch() {
	        this.isActive = false, this.clearColoredItems(), this.searchResults = [];
	      }
	      highlightSearchItem(t2) {
	        t2 && (this.currentHighlightedElement && (ne(this.currentHighlightedElement, Ge), ie(this.currentHighlightedElement, Je)), t2.page === this.controller.getCurrentPageNumber() ? this.highlightItem(t2) : this.controller.getPageMode() === e.PageMode.SinglePage ? this.clearColoredItems() : this.highlightItem(t2), this.pendingHighlightItem = t2, this.navigateToPage(t2));
	      }
	      navigateToPage(e2) {
	        this.controller.navigateToPage(e2.page, new o(e2.id, "search"));
	      }
	      colorPageElements(e2) {
	        e2 && 0 !== e2.length && (e2.forEach((e3) => {
	          let t2 = ge(this.pageContainer, "[data-search-id=" + e3.id + "]");
	          t2 && (ie(t2, Je), this.highlightedElements.push(t2));
	        }), this.highlightItem(this.pendingHighlightItem));
	      }
	      highlightItem(e2) {
	        if (e2) {
	          let t2 = this.highlightedElements.find(function(t3) {
	            return t3.dataset.searchId === e2.id;
	          });
	          t2 && (this.currentHighlightedElement = t2, ne(t2, Je), ie(t2, Ge));
	        }
	      }
	      clearColoredItems() {
	        this.highlightedElements && this.highlightedElements.length > 0 && this.highlightedElements.forEach((e2) => {
	          ne(e2, Je);
	        }), this.currentHighlightedElement && ne(this.currentHighlightedElement, Ge), this.highlightedElements = [], this.currentHighlightedElement = null;
	      }
	      applySearchColors() {
	        this.isActive && this.colorPageElements(this.searchResults);
	      }
	      onSearchComplete(e2) {
	        this.searchResults = e2, this.emit("searchComplete", e2), this.colorPageElements(e2);
	      }
	    }, e.SearchResult = class {
	      constructor() {
	        this.description = "", this.id = "", this.page = 0;
	      }
	    }, e.ServiceClient = class {
	      constructor(e2) {
	        this.connectionConfig = this.getConnectionConfig(e2), this.authStrategy = this.getAuthStrategy(this.connectionConfig);
	      }
	      getConnectionConfig(e2) {
	        return e2.reportServer && e2.reportServer.url && e2.reportServer.username && e2.reportServer.password ? new A(e2.reportServer.url, e2.reportServer.username, e2.reportServer.password) : e2.reportServer && e2.reportServer.url && e2.reportServer.getPersonalAccessToken ? new R(e2.reportServer.url, e2.reportServer.getPersonalAccessToken) : e2.reportServer && e2.reportServer.url ? new T(e2.reportServer.url) : e2.serverPreview && e2.serverShareToken ? new R("", () => Promise.resolve(e2.serverShareToken)) : new w(e2.serviceUrl);
	      }
	      getAuthStrategy(t2) {
	        switch (t2.authType) {
	          case e.AuthType.None:
	            return new P();
	          case e.AuthType.Basic:
	            return new C(t2);
	          case e.AuthType.PersonalToken:
	            return new y(t2);
	          default:
	            return new P();
	        }
	      }
	      validateClientID(e2) {
	        if (!e2)
	          throw "Invalid ClientId";
	      }
	      authenticatedGet(e2) {
	        return this.login().then((t2) => (null == t2 ? void 0 : t2.expiresAt) < Date.now() ? (this.loginPromise = this.authStrategy.authenticatePromise(true, t2.refreshToken), this.authenticatedGet(e2)) : function(e3, t3) {
	          return fetch(e3, { headers: m(t3) }).then(f);
	        }(e2, t2.access_token || t2.accessToken));
	      }
	      authenticatedPost(e2, t2) {
	        return this.login().then((i2) => (null == i2 ? void 0 : i2.expiresAt) < Date.now() ? (this.loginPromise = this.authStrategy.authenticatePromise(true, i2.refreshToken), this.authenticatedPost(e2, t2)) : v(e2, t2, i2.access_token || i2.accessToken));
	      }
	      authenticatedDelete(e2) {
	        return this.login().then((t2) => t2.expiresAt < Date.now() ? (this.loginPromise = this.authStrategy.authenticatePromise(true, t2.refreshToken), this.authenticatedDelete(e2)) : function(e3, t3) {
	          return fetch(e3, { method: "DELETE", headers: m(t3) }).then(f);
	        }(e2, t2.access_token || t2.accessToken));
	      }
	      login() {
	        return this.loginPromise || (this.loginPromise = this.authStrategy.authenticatePromise(false, null)), this.loginPromise;
	      }
	      get(e2) {
	        return fetch(e2).then((e3) => e3.text());
	      }
	      setAccessToken(e2) {
	        this.loginPromise = Promise.resolve({ accessToken: e2 });
	      }
	      registerClient() {
	        return this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients`, { timeStamp: Date.now() }).then((e2) => e2.clientId);
	      }
	      unregisterClient(e2) {
	        return this.validateClientID(e2), this.authenticatedDelete(`${this.connectionConfig.serviceUrl}/clients/${e2}`);
	      }
	      getParameters(e2, t2, i2) {
	        return this.validateClientID(e2), this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/parameters`, { report: t2, parameterValues: i2 || {} }).then((e3) => e3);
	      }
	      createReportInstance(e2, t2, i2) {
	        return this.validateClientID(e2), this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances`, { report: t2, parameterValues: i2 }).then((e3) => e3.instanceId);
	      }
	      deleteReportInstance(e2, t2) {
	        return this.validateClientID(e2), this.authenticatedDelete(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}`);
	      }
	      createReportDocument(e2, t2, i2, n2, r2, s2, o2) {
	        return this.validateClientID(e2), n2.BasePath = this.connectionConfig.serviceUrl, this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents`, { format: i2, deviceInfo: n2, useCache: r2, baseDocumentID: s2, actionID: o2 }).then((e3) => e3.documentId);
	      }
	      sendDocument(e2, t2, i2, n2) {
	        return this.validateClientID(e2), this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}/send`, n2);
	      }
	      deleteReportDocument(e2, t2, i2) {
	        return this.validateClientID(e2), this.authenticatedDelete(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}`);
	      }
	      getDocumentUrl(e2, t2, i2) {
	        return `${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}`;
	      }
	      getDocumentInfo(e2, t2, i2) {
	        return this.validateClientID(e2), this.authenticatedGet(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}/info`).then((e3) => e3);
	      }
	      getPage(e2, t2, i2, n2) {
	        return this.validateClientID(e2), this.authenticatedGet(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}/pages/${n2}`).then((e3) => {
	          let t3 = e3;
	          return t3.pageActions = JSON.parse(e3.pageActions), t3;
	        });
	      }
	      getDocumentFormats() {
	        return this.authenticatedGet(`${this.connectionConfig.serviceUrl}/formats`).then((e2) => e2 || []);
	      }
	      getServiceVersion() {
	        return this.authenticatedGet(`${this.connectionConfig.serviceUrl}/version`).then((e2) => e2.toString());
	      }
	      getServiceConfiguration() {
	        return this.authenticatedGet(`${this.connectionConfig.serviceUrl}/configuration`).then((e2) => e2);
	      }
	      getResource(e2, t2, i2, n2) {
	        return this.validateClientID(e2), this.authenticatedGet(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}/resources/${n2}`);
	      }
	      getSearchResults(e2, t2, i2, n2) {
	        return this.validateClientID(e2), this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}/search`, n2).then(function(e3) {
	          return e3 || [];
	        });
	      }
	      createAIThread(e2, t2, i2, n2) {
	        return this.validateClientID(e2), this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/ai`, { report: i2, parameterValues: n2 }).then((e3) => e3);
	      }
	      getAIResponse(e2, t2, i2, n2, r2) {
	        return this.validateClientID(e2), this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/${e2}/instances/${t2}/documents/${i2}/ai/${n2}/query`, { query: r2 }).then(function(e3) {
	          return e3 || "";
	        });
	      }
	      getServiceUrl() {
	        return this.connectionConfig.serviceUrl;
	      }
	      keepClientAlive(e2) {
	        return this.authenticatedPost(`${this.connectionConfig.serviceUrl}/clients/keepAlive/${e2}`, {});
	      }
	      getClientsSessionTimeoutSeconds() {
	        return this.authenticatedGet(`${this.connectionConfig.serviceUrl}/clients/sessionTimeout`).then(function(e2) {
	          return e2.clientSessionTimeout;
	        });
	      }
	    }, e.TooltipEventArgs = p, e.addClass = ie, e.appendHtml = ue, e.createElement = te, e.debounce = ve, e.each = we, e.escapeHtml = le, e.findElement = ge, e.getColorAlphaValue = de, e.getElementAttributeValue = me, e.getElementScrollParent = fe, e.getOffsetParent = se, e.hasClass = re, e.isArray = Te, e.isExceptionOfType = Ce, e.isInternalServerError = Ie, e.isInvalidClientException = ye, e.isInvalidParameterException = Se, e.isRgbColor = ce, e.keepElementInView = function(e2) {
	      if (!e2)
	        return;
	      let t2 = fe(e2);
	      if (!t2)
	        return;
	      let i2 = e2.offsetTop, n2 = i2 + e2.offsetHeight, r2 = t2.scrollTop + t2.offsetHeight;
	      i2 < t2.scrollTop ? t2.scrollTop = i2 : n2 > r2 && (t2.scrollTop += n2 - r2);
	    }, e.parseToLocalDate = Ee, e.prependHtml = pe, e.removeClass = ne, e.reportSourcesAreEqual = function(e2) {
	      const t2 = e2.firstReportSource, i2 = e2.secondReportSource;
	      if (t2 && i2 && t2.report === i2.report) {
	        let e3 = "";
	        t2.parameters && (e3 = JSON.stringify(t2.parameters));
	        let n2 = "";
	        return i2.parameters && (n2 = JSON.stringify(i2.parameters)), e3 === n2;
	      }
	      return false;
	    }, e.scaleElement = ae, e.stringFormat = be, e.throttle = Pe, e.toPixel = oe, e.toRgbColor = he, e.tryParseFloat = Re, e.tryParseInt = Ae;
	  });
	})(dist, dist.exports);
	var distExports = dist.exports;

	var stringFormatRegExp = /{(\w+?)}/g;
	var specialKeys = {
	  DELETE: 46,
	  BACKSPACE: 8,
	  TAB: 9,
	  ESC: 27,
	  LEFT: 37,
	  UP: 38,
	  RIGHT: 39,
	  DOWN: 40,
	  END: 35,
	  HOME: 36
	};
	function isSpecialKey() {
	  var userAgent = window.navigator.userAgent.toLowerCase();
	  if (userAgent.indexOf("firefox") > -1) {
	    var specialKeysArray = Object.keys(specialKeys);
	    var specialKeysLength = specialKeysArray.length;
	    return function(keyCode) {
	      for (var i = 0; i < specialKeysLength; i++) {
	        if (specialKeys[specialKeysArray[i]] == keyCode) {
	          return true;
	        }
	      }
	    };
	  }
	  return function(keyCode) {
	    return false;
	  };
	}
	function rectangle(left, top, width, height) {
	  return {
	    left,
	    top,
	    width,
	    height,
	    right: function() {
	      return left + width;
	    },
	    bottom: function() {
	      return top + height;
	    },
	    union: function(other) {
	      var newLeft = Math.min(left, other.left);
	      var newTop = Math.min(top, other.top);
	      var newWidth = Math.max(this.right(), other.right()) - newLeft;
	      var newHeight = Math.max(this.bottom(), other.bottom()) - newTop;
	      return rectangle(
	        newLeft,
	        newTop,
	        newWidth,
	        newHeight
	      );
	    }
	  };
	}
	function generateGuidString() {
	  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function(c) {
	    var r = Math.random() * 16 | 0;
	    var v = c == "x" ? r : r & 3 | 8;
	    return v.toString(16);
	  });
	}
	function trim(s, charlist) {
	  return rTrim(lTrim(s, charlist), charlist);
	}
	function replaceAll(str, find, replace) {
	  return str.replace(new RegExp(find, "g"), replace);
	}
	function lTrim(s, charlist) {
	  if (charlist === void 0) {
	    charlist = "s";
	  }
	  return s.replace(new RegExp("^[" + charlist + "]+"), "");
	}
	function rTrim(s, charlist) {
	  if (charlist === void 0) {
	    charlist = "s";
	  }
	  return s.replace(new RegExp("[" + charlist + "]+$"), "");
	}
	function stringFormat(template, data) {
	  var isArray = Array.isArray(data);
	  return template.replace(stringFormatRegExp, function($0, $1) {
	    return data[isArray ? parseInt($1) : $1];
	  });
	}
	function escapeHtml(str) {
	  return $("<div>").text(str).html();
	}
	function adjustTimezone(date) {
	  return new Date(
	    Date.UTC(
	      date.getFullYear(),
	      date.getMonth(),
	      date.getDate(),
	      date.getHours(),
	      date.getMinutes(),
	      date.getSeconds(),
	      date.getMilliseconds()
	    )
	  );
	}
	function unadjustTimezone(date) {
	  return new Date(
	    date.getUTCFullYear(),
	    date.getUTCMonth(),
	    date.getUTCDate(),
	    date.getUTCHours(),
	    date.getUTCMinutes(),
	    date.getUTCSeconds(),
	    date.getUTCMilliseconds()
	  );
	}
	function areEqual(v1, v2) {
	  if (v1 instanceof Date && v2 instanceof Date) {
	    if (v1.getTime() !== v2.getTime()) {
	      return false;
	    }
	  } else if (v1 !== v2) {
	    return false;
	  }
	  return true;
	}
	function areEqualArrays(array1, array2) {
	  if (array1 === null) {
	    if (array2 !== null) {
	      return false;
	    }
	    return true;
	  }
	  if (array2 === null) {
	    return false;
	  }
	  if (array1.length !== array2.length) {
	    return false;
	  }
	  for (var j = array1.length - 1; j >= 0; j--) {
	    if (!areEqual(array1[j], array2[j])) {
	      return false;
	    }
	  }
	  return true;
	}
	function isApplicationException(xhr) {
	  return isApplicationExceptionInstance(getExceptionInstance(xhr));
	}
	function isApplicationExceptionInstance(exception) {
	  var exceptionShortName = "DrawingFactoryUnavailableException";
	  return isExceptionInstanceOfType(exception, exceptionShortName, "Telerik.Drawing.Contract." + exceptionShortName);
	}
	function isExceptionInstanceOfType(exceptionInstance, exceptionTypeShortName, exceptionTypeFullName) {
	  return exceptionInstance && exceptionInstance.exceptionType && exceptionTypeNamesMatch(exceptionInstance.exceptionType, exceptionTypeShortName, exceptionTypeFullName);
	}
	function exceptionTypeNamesMatch(instanceTypeName, exceptionTypeShortName, exceptionTypeFullName) {
	  return instanceTypeName && (instanceTypeName === exceptionTypeFullName || instanceTypeName.endsWith(exceptionTypeShortName));
	}
	function parseJSON(json) {
	  try {
	    return JSON.parse(
	      json,
	      function(key, value) {
	        if (key && value) {
	          var firstChar = key.charAt(0);
	          if (firstChar === firstChar.toUpperCase()) {
	            var newPropertyName = firstChar.toLowerCase() + key.slice(1);
	            this[newPropertyName] = value;
	          }
	        }
	        return value;
	      }
	    );
	  } catch (e) {
	    return null;
	  }
	}
	function getExceptionInstance(xhr) {
	  if (!xhr || !xhr.responseText) {
	    return false;
	  }
	  return parseJSON(xhr.responseText);
	}
	function filterUniqueLastOccurrence(array) {
	  function onlyLastUnique(value, index, self) {
	    return self.lastIndexOf(value) === index;
	  }
	  return array.filter(onlyLastUnique);
	}
	function logError(error) {
	  var console = window.console;
	  if (console && console.error) {
	    console.error(error);
	  }
	}
	function toRgbColor(hexColor) {
	  if (hexColor && hexColor.length < 6) {
	    var index = 1;
	    var hexParts = hexColor.split("");
	    if (hexParts[0] !== "#") {
	      index = 0;
	    }
	    for (index; index < hexParts.length; index++) {
	      hexParts[index] = hexParts[index] + hexParts[index];
	    }
	    hexColor = hexParts.join("");
	  }
	  var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hexColor);
	  return result ? parseInt(result[1], 16) + ", " + parseInt(result[2], 16) + ", " + parseInt(result[3], 16) : null;
	}
	function isRgbColor(color) {
	  if (!color) {
	    return false;
	  }
	  return color.indexOf(",") > -1 ? true : false;
	}
	function getColorAlphaValue(color) {
	  if (color.toLowerCase() === "transparent") {
	    return 0;
	  }
	  if (!isRgbColor(color)) {
	    return 1;
	  }
	  if (color.indexOf("#") !== -1) {
	    color = toRgbColor(color);
	  }
	  var colorComponents = color.split(",").map(function(c) {
	    return c.trim();
	  });
	  var alpha = colorComponents.length === 4 ? parseFloat((parseFloat(colorComponents[3].replace(/[()]/g, "")) / 255).toFixed(2)) : 1;
	  return alpha;
	}

	const utils = /*#__PURE__*/Object.freeze(/*#__PURE__*/Object.defineProperty({
		__proto__: null,
		adjustTimezone,
		areEqual,
		areEqualArrays,
		escapeHtml,
		exceptionTypeNamesMatch,
		filterUniqueLastOccurrence,
		generateGuidString,
		getColorAlphaValue,
		getExceptionInstance,
		isApplicationException,
		isApplicationExceptionInstance,
		isRgbColor,
		isSpecialKey,
		lTrim,
		logError,
		parseJSON,
		parseToLocalDate: distExports.parseToLocalDate,
		rTrim,
		rectangle,
		replaceAll,
		reportSourcesAreEqual: distExports.reportSourcesAreEqual,
		stringFormat,
		toRgbColor,
		trim,
		tryParseFloat: distExports.tryParseFloat,
		tryParseInt: distExports.tryParseInt,
		unadjustTimezone
	}, Symbol.toStringTag, { value: 'Module' }));

	function toPixels(value) {
	  return parseInt(value, 10) || 0;
	}
	function getMargins(dom) {
	  var $target = $(dom);
	  return {
	    left: toPixels($target.css("marginLeft")),
	    right: toPixels($target.css("marginRight")),
	    top: toPixels($target.css("marginTop")),
	    bottom: toPixels($target.css("marginBottom"))
	  };
	}
	function getPadding(dom) {
	  var $target = $(dom);
	  return {
	    left: toPixels($target.css("paddingLeft")),
	    right: toPixels($target.css("paddingRight")),
	    top: toPixels($target.css("paddingTop")),
	    bottom: toPixels($target.css("paddingBottom"))
	  };
	}
	function getBorderWidth(dom) {
	  var $target = $(dom);
	  return {
	    left: toPixels($target.css("borderLeftWidth")),
	    right: toPixels($target.css("borderRightWidth")),
	    top: toPixels($target.css("borderTopWidth")),
	    bottom: toPixels($target.css("borderBottomWidth"))
	  };
	}
	function scale(dom, scaleX, scaleY, originX, originY) {
	  scaleX = scaleX || 1;
	  scaleY = scaleY || 1;
	  originX = originX || 0;
	  originY = originY || 0;
	  var scale2 = stringFormat("scale({0}, {1})", [scaleX, scaleY]);
	  var origin = stringFormat("{0} {1}", [originX, originY]);
	  $(dom).css("transform", scale2).css("transform-origin", origin);
	}

	const domUtils = /*#__PURE__*/Object.freeze(/*#__PURE__*/Object.defineProperty({
		__proto__: null,
		getBorderWidth,
		getMargins,
		getPadding,
		scale
	}, Symbol.toStringTag, { value: 'Module' }));

	var defaultOptions$5 = {};
	function Accessibility(options) {
	  var controller;
	  var areas;
	  var lastArea;
	  var keyMap = {
	    CONFIRM_KEY: 13,
	    // C
	    CONTENT_AREA_KEY: 67,
	    // D
	    DOCUMENT_MAP_AREA_KEY: 68,
	    // M
	    MENU_AREA_KEY: 77,
	    // P
	    PARAMETERS_AREA_KEY: 80
	  };
	  options = $.extend({}, defaultOptions$5, options);
	  controller = options.controller;
	  if (!controller) {
	    throw "No controller (telerikReporting.ReportViewerController) has been specified.";
	  }
	  init();
	  function init() {
	    _attachEvents();
	  }
	  function _attachEvents() {
	    controller.onAsync("reportLoadComplete", async () => {
	      setAccessibilityUI();
	      var content = findContentArea();
	      if (content.length > 0) {
	        content.focus();
	      }
	    }).on("pageReady", (pageInfo) => {
	      initPage(pageInfo);
	    }).on("error", (event, message) => {
	      focusOnErrorMessage();
	      window.setTimeout(setAccessibilityUI, 500);
	    });
	  }
	  function setAccessibilityUI() {
	    if (!areas) {
	      initAreas();
	      $(document.body).off("keydown", processKeyDown);
	      $(document.body).on("keydown", processKeyDown);
	    }
	  }
	  function focusOnErrorMessage() {
	    var $errMsg = $("div.trv-pages-area div.trv-error-message");
	    if ($errMsg.length === 0) {
	      return;
	    }
	    $errMsg.attr("tabIndex", 0);
	    $errMsg.focus();
	  }
	  function initPage(pageInfo) {
	    if (!pageInfo) {
	      return;
	    }
	    setAccessibilityUI();
	    var area = areas[keyMap.CONTENT_AREA_KEY];
	    setContentAreaKeyDown(area);
	  }
	  function initAreas() {
	    areas = {};
	    areas[keyMap.DOCUMENT_MAP_AREA_KEY] = findDocumentMapArea();
	    areas[keyMap.MENU_AREA_KEY] = findMenuArea();
	    areas[keyMap.CONTENT_AREA_KEY] = findContentArea();
	    var parametersArea = findParametersArea();
	    if (parametersArea) {
	      areas[keyMap.PARAMETERS_AREA_KEY] = parametersArea;
	      setParameterEditorsKeyDown(parametersArea);
	    }
	  }
	  function findContentArea() {
	    return $("div[data-role=telerik_ReportViewer_PagesArea]");
	  }
	  function findDocumentMapArea() {
	    return $("div[data-role=telerik_ReportViewer_DocumentMapArea] div[data-role=treeview]");
	  }
	  function findMenuArea() {
	    return $("div[data-role=telerik_ReportViewer_Toolbar]");
	  }
	  function findParametersArea() {
	    return $("div[data-role=telerik_ReportViewer_ParametersArea] .trv-parameters-area-content .trv-parameters-wrapper");
	  }
	  function processKeyDown(event) {
	    if (!areas) {
	      return;
	    }
	    if (!(event.altKey && event.ctrlKey)) {
	      return;
	    }
	    var currentArea = areas[event.which];
	    if (!currentArea) {
	      return;
	    }
	    if (!IsAreaContainerVisible(currentArea.parent())) {
	      return;
	    }
	    var className = "k-focus";
	    if (lastArea) {
	      lastArea.removeClass(className);
	    }
	    currentArea.addClass(className);
	    currentArea.focus();
	    lastArea = currentArea;
	    event.preventDefault();
	  }
	  function setParameterEditorsKeyDown(parametersAreaContent) {
	    if (parametersAreaContent.length === 0) {
	      return;
	    }
	    var $paramsArea = parametersAreaContent.parent("div[data-role=telerik_ReportViewer_ParametersArea]");
	    if (!IsAreaContainerVisible($paramsArea)) {
	      return;
	    }
	    Array.from(parametersAreaContent.children()).forEach((child) => {
	      $(child).on("keydown", (event) => {
	        if (event.which == keyMap.CONFIRM_KEY) {
	          var paramsButton = $paramsArea.find("button.trv-parameters-area-preview-button");
	          paramsButton.focus();
	          event.preventDefault();
	        }
	      });
	    });
	  }
	  function IsAreaContainerVisible(container) {
	    return container && !(container.hasClass("k-collapsed") || container.hasClass("k-hidden"));
	  }
	  function setContentAreaKeyDown(contentArea) {
	    if (!contentArea) {
	      return;
	    }
	    var actions = contentArea.find("div [data-reporting-action]");
	    if (!actions.length > 0) {
	      return;
	    }
	    Array.from(actions).forEach((action) => {
	      var $action = $(action);
	      $action.on("keydown", (event) => {
	        if (event.which == keyMap.CONFIRM_KEY) {
	          $action.trigger("click");
	        }
	      });
	    });
	  }
	  function setKeyMap(keyMapValues) {
	    keyMap = keyMapValues;
	    areas = void 0;
	  }
	  function getKeyMap() {
	    return keyMap;
	  }
	  return {
	    getKeyMap,
	    setKeyMap
	  };
	}

	const GlobalSettings = {
	  viewerInstances: []
	};

	class Binder {
	  static bind($element, ...args) {
	    const commands = args[0].commands;
	    const viewerOptions = args[1];
	    Binder.attachCommands($element, commands, viewerOptions);
	    var plugins = $element.find('[data-role^="telerik_ReportViewer_"]');
	    Array.from(plugins).forEach((element) => {
	      var $element2 = $(element);
	      var fn = $.fn[$element2.attr("data-role")];
	      if (typeof fn === "function") {
	        fn.apply($element2, args);
	      }
	    });
	  }
	  static attachCommands($element, commands, viewerOptions) {
	    var elementSelector = '[data-command^="telerik_ReportViewer_"]';
	    var customElementSelector = "[data-target-report-viewer]" + elementSelector;
	    $element.on("click", elementSelector, commandHandler);
	    if (!GlobalSettings.CommandHandlerAttached) {
	      $(document.body).on("click", customElementSelector, customCommandHandler);
	      GlobalSettings.CommandHandlerAttached = true;
	    }
	    Object.entries(commands).forEach(([key, command]) => {
	      attachCommand(key, command, viewerOptions, $element);
	    });
	    function commandHandler(event) {
	      var prefixedDataCommand = $(event.currentTarget).attr("data-command");
	      if (prefixedDataCommand) {
	        var dataCommand = prefixedDataCommand.substring("telerik_ReportViewer_".length);
	        var cmd = commands[dataCommand];
	        if (cmd && cmd.getEnabled()) {
	          cmd.exec($(event.currentTarget).attr("data-command-parameter"));
	        }
	        event.preventDefault();
	      }
	    }
	    function customCommandHandler(event) {
	      var $this = $(event.currentTarget);
	      var prefixedDataCommand = $this.attr("data-command");
	      var reportViewerTarget = $this.attr("data-target-report-viewer");
	      if (prefixedDataCommand && reportViewerTarget) {
	        var dataCommand = prefixedDataCommand.substring("telerik_ReportViewer_".length);
	        var reportViewer = $(reportViewerTarget).data("telerik_ReportViewer");
	        var cmd = reportViewer.commands[dataCommand];
	        if (cmd.getEnabled()) {
	          cmd.exec($(event.currentTarget).attr("data-command-parameter"));
	        }
	        event.preventDefault();
	      }
	    }
	  }
	}
	function attachCommand(dataCommand, cmd, viewerOptions, $element) {
	  if (cmd) {
	    var elementSelector = '[data-command="telerik_ReportViewer_' + dataCommand + '"]';
	    var customElementSelector = '[data-target-report-viewer="' + viewerOptions.selector + '"]' + elementSelector;
	    var $defaultElement = $element.find(elementSelector);
	    var $customElement = $(customElementSelector);
	    $(cmd).on("enabledChanged", (event) => {
	      (cmd.getEnabled() ? $.fn.removeClass : $.fn.addClass).call($defaultElement.parent("li"), "k-disabled");
	      (cmd.getEnabled() ? $.fn.removeClass : $.fn.addClass).call($customElement, viewerOptions.disabledButtonClass);
	    }).on("checkedChanged", (event) => {
	      var defaultTarget = $defaultElement.parent("li");
	      (cmd.getChecked() ? $.fn.addClass : $.fn.removeClass).call(defaultTarget, getSelectedClassName(defaultTarget));
	      (cmd.getChecked() ? $.fn.addClass : $.fn.removeClass).call($customElement, viewerOptions.checkedButtonClass);
	    });
	  }
	}
	function getSelectedClassName($element) {
	  return $element.hasClass("trv-menu-toggleable") ? "k-selected !k-bg-primary" : "k-selected";
	}

	class Command {
	  // #region fields
	  _enabled;
	  _checked;
	  _fn;
	  // #endregion
	  // #region properties
	  getEnabled() {
	    return this._enabled;
	  }
	  setEnabled(value) {
	    const newState = Boolean(value);
	    if (this._enabled !== newState) {
	      this._enabled = newState;
	    }
	    $(this).trigger("enabledChanged", this._enabled);
	    return this;
	  }
	  getChecked() {
	    return this._checked;
	  }
	  setChecked(value) {
	    const newState = Boolean(value);
	    if (this._checked !== newState) {
	      this._checked = newState;
	    }
	    $(this).trigger("checkedChanged", this._checked);
	    return this;
	  }
	  // #endregion
	  // #region constructor
	  constructor(fn) {
	    this._enabled = true;
	    this._checked = false;
	    this._fn = fn;
	  }
	  // #endregion
	  // #region methods
	  exec(...args) {
	    if (typeof this._fn === "function") {
	      this._fn.apply(this, args);
	    }
	  }
	  // #endregion
	}

	var scaleTransitionMap = {};
	scaleTransitionMap[distExports.ScaleMode.FitPage] = distExports.ScaleMode.FitPageWidth;
	scaleTransitionMap[distExports.ScaleMode.FitPageWidth] = distExports.ScaleMode.Specific;
	scaleTransitionMap[distExports.ScaleMode.Specific] = distExports.ScaleMode.FitPage;
	var scaleValues = [0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 4, 8];
	function CommandSet(options) {
	  var controller = options.controller;
	  if (!controller) {
	    throw "No options.controller.";
	  }
	  var notificationService = options.notificationService;
	  if (!notificationService) {
	    throw "No options.notificationService.";
	  }
	  var historyManager = options.history;
	  if (!historyManager) {
	    throw "No options.history.";
	  }
	  function getDocumentMapVisible() {
	    var args = {};
	    notificationService.getDocumentMapState(args);
	    return Boolean(args.visible);
	  }
	  function getParametersAreaVisible() {
	    var args = {};
	    notificationService.getParametersAreaState(args);
	    return Boolean(args.visible);
	  }
	  function getSearchDialogVisible() {
	    return Boolean(controller.getSearchInitiated());
	  }
	  function getSendEmailDialogVisible() {
	    var args = {};
	    notificationService.getSendEmailDialogState(args);
	    return Boolean(args.visible);
	  }
	  function getAiPromptDialogVisible() {
	    return Boolean(controller.getAiPromptInitiated());
	  }
	  return {
	    "historyBack": new Command(() => {
	      historyManager.back();
	    }),
	    "historyForward": new Command(() => {
	      historyManager.forward();
	    }),
	    "stopRendering": new Command(() => {
	      controller.stopRendering();
	    }),
	    "goToPrevPage": new Command(() => {
	      controller.navigateToPage(controller.getCurrentPageNumber() - 1);
	    }),
	    "goToNextPage": new Command(() => {
	      controller.navigateToPage(controller.getCurrentPageNumber() + 1);
	    }),
	    "goToFirstPage": new Command(() => {
	      controller.navigateToPage(1);
	    }),
	    "goToLastPage": new Command(() => {
	      controller.navigateToPage(controller.getPageCount());
	    }),
	    "goToPage": new Command((pageNumber) => {
	      if (!isNaN(pageNumber)) {
	        var pageCount = controller.getPageCount();
	        if (pageNumber > pageCount) {
	          pageNumber = pageCount;
	        } else if (pageNumber < 1) {
	          pageNumber = 1;
	        }
	        controller.navigateToPage(pageNumber);
	        return pageNumber;
	      }
	    }),
	    "refresh": new Command(() => {
	      controller.refreshReport(true, "", false);
	    }),
	    "export": new Command((format) => {
	      if (format) {
	        controller.exportReport(format);
	      }
	    }),
	    "print": new Command(() => {
	      controller.printReport();
	    }),
	    "pageMode": new Command(() => {
	      controller.setPageMode(
	        controller.getPageMode() === distExports.PageMode.SinglePage ? distExports.PageMode.ContinuousScroll : distExports.PageMode.SinglePage
	      );
	      controller.refreshReportCore(false, controller.getReportDocumentId());
	    }),
	    "togglePrintPreview": new Command(() => {
	      controller.setViewMode(
	        controller.getViewMode() === distExports.ViewMode.PrintPreview ? distExports.ViewMode.Interactive : distExports.ViewMode.PrintPreview
	      );
	      controller.refreshReportCore(false, controller.getReportDocumentId());
	    }),
	    "toggleDocumentMap": new Command(() => {
	      notificationService.setDocumentMapVisible({ visible: !getDocumentMapVisible() });
	    }),
	    "toggleParametersArea": new Command(() => {
	      notificationService.setParametersAreaVisible({ visible: !getParametersAreaVisible() });
	    }),
	    "zoom": new Command((scale) => {
	      controller.setScaleMode(distExports.ScaleMode.Specific);
	      controller.setScale(scale);
	    }),
	    "zoomIn": new Command(() => {
	      zoom(1);
	    }),
	    "zoomOut": new Command(() => {
	      zoom(-1);
	    }),
	    "toggleZoomMode": new Command((e) => {
	      var scaleMode = controller.getScaleMode();
	      var newScaleMode = scaleTransitionMap[scaleMode];
	      controller.setScaleMode(newScaleMode);
	      if (newScaleMode === distExports.ScaleMode.Specific) {
	        controller.setScale(1);
	      }
	    }),
	    "toggleSearchDialog": new Command(() => {
	      notificationService.setSearchDialogVisible({ visible: !getSearchDialogVisible() });
	    }),
	    "toggleSendEmailDialog": new Command(() => {
	      notificationService.setSendEmailDialogVisible({ visible: !getSendEmailDialogVisible() });
	    }),
	    "toggleAiPromptDialog": new Command(() => {
	      notificationService.setAiPromptDialogVisible({ visible: !getAiPromptDialogVisible() });
	    })
	  };
	  function zoom(step) {
	    var scale = getZoomScale(controller.getScale(), step);
	    controller.setScaleMode(distExports.ScaleMode.Specific);
	    controller.setScale(scale);
	  }
	  function getZoomScale(scale, steps) {
	    var pos = -1;
	    var length = scaleValues.length;
	    for (var i = 0; i < length; i++) {
	      var value = scaleValues[i];
	      if (scale < value) {
	        pos = i - 0.5;
	        break;
	      }
	      if (scale === value) {
	        pos = i;
	        break;
	      }
	    }
	    pos = pos + steps;
	    if (steps >= 0) {
	      pos = Math.round(pos - 0.49);
	    } else {
	      pos = Math.round(pos + 0.49);
	    }
	    if (pos < 0) {
	      pos = 0;
	    } else if (pos > length - 1) {
	      pos = length - 1;
	    }
	    return scaleValues[pos];
	  }
	}

	const ViewModes = {
	  INTERACTIVE: "INTERACTIVE",
	  PRINT_PREVIEW: "PRINT_PREVIEW"
	};
	const PrintModes = {
	  AUTO_SELECT: "AUTO_SELECT",
	  FORCE_PDF_PLUGIN: "FORCE_PDF_PLUGIN",
	  FORCE_PDF_FILE: "FORCE_PDF_FILE"
	};
	const PageModes = {
	  SINGLE_PAGE: "SINGLE_PAGE",
	  CONTINUOUS_SCROLL: "CONTINUOUS_SCROLL"
	};
	const ScaleModes = {
	  FIT_PAGE_WIDTH: "FIT_PAGE_WIDTH",
	  FIT_PAGE: "FIT_PAGE",
	  SPECIFIC: "SPECIFIC"
	};
	const ParameterTypes = {
	  INTEGER: "System.Int64",
	  FLOAT: "System.Double",
	  STRING: "System.String",
	  DATETIME: "System.DateTime",
	  BOOLEAN: "System.Boolean"
	};
	const ParameterEditorTypes = {
	  COMBO_BOX: "COMBO_BOX",
	  LIST_VIEW: "LIST_VIEW"
	};
	const ParametersAreaPositions = {
	  RIGHT: "RIGHT",
	  LEFT: "LEFT",
	  TOP: "TOP",
	  BOTTOM: "BOTTOM"
	};
	const DocumentMapAreaPositions = {
	  RIGHT: "RIGHT",
	  LEFT: "LEFT"
	};

	const Events$1 = {
	  EXPORT_BEGIN: "EXPORT_BEGIN",
	  EXPORT_END: "EXPORT_END",
	  PRINT_BEGIN: "PRINT_BEGIN",
	  PRINT_END: "PRINT_END",
	  RENDERING_BEGIN: "RENDERING_BEGIN",
	  RENDERING_END: "RENDERING_END",
	  PAGE_READY: "PAGE_READY",
	  ERROR: "ERROR",
	  UPDATE_UI: "UPDATE_UI",
	  INTERACTIVE_ACTION_EXECUTING: "INTERACTIVE_ACTION_EXECUTING",
	  INTERACTIVE_ACTION_ENTER: "INTERACTIVE_ACTION_ENTER",
	  INTERACTIVE_ACTION_LEAVE: "INTERACTIVE_ACTION_LEAVE",
	  VIEWER_TOOLTIP_OPENING: "VIEWER_TOOLTIP_OPENING",
	  SEND_EMAIL_BEGIN: "SEND_EMAIL_BEGIN",
	  SEND_EMAIL_END: "SEND_EMAIL_END",
	  PARAMETERS_LOADED: "PARAMETERS_LOADED"
	};

	function HistoryManager(options) {
	  var controller = options.controller;
	  if (!controller) {
	    throw "No controller (telerikReporting.reportViewerController) has been specified.";
	  }
	  var settings = options.settings;
	  var history = settings.getHistory() || { records: [], position: -1 };
	  controller.on("loadedReportChange", () => {
	    addToHistory(true);
	  }).on("currentPageChanged", () => {
	    updatePageInfo();
	  }).onAsync("reportLoadComplete", async () => {
	    addToHistory(false);
	  }).on("clientExpired", () => {
	    var records = history.records;
	    for (var i = 0; i < records.length; i++) {
	      records[i].reportDocumentId = null;
	    }
	  });
	  function getCurrentRecord() {
	    var records = history.records;
	    if (records.length > 0) {
	      return records[history.position];
	    }
	    return null;
	  }
	  function pushRecord(rec) {
	    var records = history.records;
	    var position = history.position;
	    records = Array.prototype.slice.call(records, 0, position + 1);
	    records.push(rec);
	    history.records = records;
	    history.position = records.length - 1;
	    saveSettings();
	  }
	  function saveSettings() {
	    settings.setHistory(history);
	  }
	  function updatePageInfo() {
	    var currentRecord = getCurrentRecord();
	    if (currentRecord) {
	      currentRecord.pageNumber = controller.getCurrentPageNumber();
	      currentRecord.viewMode = controller.getViewMode();
	      currentRecord.reportDocumentId = controller.getReportDocumentId();
	      saveSettings();
	    }
	  }
	  function addToHistory(temp) {
	    removeTempRecordsFromHistory();
	    var currentRecord = getCurrentRecord();
	    var rs = controller.getReportSource();
	    var reportSources = { firstReportSource: currentRecord?.reportSource, secondReportSource: rs };
	    if (!currentRecord || !distExports.reportSourcesAreEqual(reportSources)) {
	      pushRecord({
	        reportSource: rs,
	        pageNumber: 1,
	        temp
	      });
	    }
	  }
	  function exec(currentRecord, reportChanged) {
	    controller.setReportDocumentId(currentRecord.reportDocumentId);
	    controller.setViewMode(currentRecord.viewMode);
	    controller.setReportSource(currentRecord.reportSource);
	    controller.refreshReport(false, currentRecord.reportDocumentId, reportChanged);
	    controller.navigateToPage(currentRecord.pageNumber);
	  }
	  function canMove(step) {
	    var position = history.position;
	    var length = history.records.length;
	    var newPos = position + step;
	    return 0 <= newPos && newPos < length;
	  }
	  function move(step) {
	    var position = history.position;
	    var length = history.records.length;
	    var newPos = position + step;
	    if (newPos < 0) {
	      newPos = 0;
	    } else if (newPos >= length) {
	      newPos = length - 1;
	    }
	    if (newPos != position) {
	      var currentRecord = getCurrentRecord();
	      var currentReport = currentRecord.reportSource ? currentRecord.reportSource.report : "";
	      history.position = newPos;
	      saveSettings();
	      var newRecord = getCurrentRecord();
	      var newReport = newRecord.reportSource ? newRecord.reportSource.report : "";
	      exec(newRecord, currentReport !== newReport);
	    }
	  }
	  function removeTempRecordsFromHistory() {
	    var lastIndex = history.records.length - 1;
	    while (lastIndex >= 0) {
	      if (history.records[lastIndex].temp === true) {
	        history.records.splice(lastIndex, 1);
	        if (history.position >= lastIndex) {
	          history.position--;
	        }
	      } else {
	        break;
	      }
	      lastIndex--;
	    }
	  }
	  return {
	    back: () => {
	      move(-1);
	    },
	    forward: () => {
	      move(1);
	    },
	    canMoveBack: () => {
	      return canMove(-1);
	    },
	    canMoveForward: () => {
	      return canMove(1);
	    },
	    loadCurrent: () => {
	      var rec = getCurrentRecord();
	      if (rec) {
	        exec(rec, false);
	      }
	      return Boolean(rec);
	    }
	  };
	}

	var sr$1 = {
	  // warning and error string resources
	  controllerNotInitialized: "Controller is not initialized.",
	  noReportInstance: "No report instance.",
	  missingTemplate: "!obsolete resource!",
	  noReport: "No report.",
	  noReportDocument: "No report document.",
	  missingOrInvalidParameter: "There are missing or invalid parameter values. Please input valid data for the following parameters:\n",
	  invalidParameter: "Please input a valid value.",
	  invalidDateTimeValue: "Please input a valid date.",
	  parameterIsEmpty: "Parameter value cannot be empty.",
	  cannotValidateType: "Cannot validate parameter of type {type}.",
	  loadingFormats: "Loading...",
	  loadingReport: "Loading report...",
	  loadingParameters: "Loading parameters...",
	  autoRunDisabled: "Please validate the report parameter values and press Preview to generate the report.",
	  preparingDownload: "Preparing document to download. Please wait...",
	  preparingPrint: "Preparing document to print. Please wait...",
	  errorLoadingTemplates: "Error loading the report viewer's templates. (templateUrl = '{0}').",
	  errorServiceUrl: "Cannot access the Reporting REST service. (serviceUrl = '{0}'). Make sure the service address is correct and enable CORS if needed. (https://enable-cors.org)",
	  errorServiceVersion: "The version of the Report Viewer '{1}' does not match the version of the Reporting REST Service '{0}'. Please make sure both are running same version.",
	  loadingReportPagesInProgress: "{0} pages loaded so far...",
	  loadedReportPagesComplete: "Done. Total {0} pages loaded.",
	  noPageToDisplay: "No page to display.",
	  errorDeletingReportInstance: "Error deleting report instance: '{0}'.",
	  errorRegisteringViewer: "Error registering the viewer with the service.",
	  noServiceClient: "No serviceClient has been specified for this controller.",
	  errorRegisteringClientInstance: "Error registering client instance.",
	  errorCreatingReportInstance: "Error creating report instance (Report = '{0}').",
	  errorCreatingReportDocument: "Error creating report document (Report = '{0}'; Format = '{1}').",
	  unableToGetReportParameters: "Unable to get report parameters.",
	  errorUnauthorizedOrForbidden: "You don't have permission to access this report document.",
	  errorObtainingAuthenticationToken: "Error obtaining authentication token.",
	  clientExpired: "Click 'Refresh' to restore client session.",
	  promisesChainStopError: "Error shown. Throwing promises chain stop error.",
	  renderingCancelled: "Report processing was canceled.",
	  tryReportPreview: "The report may now be previewed.",
	  // viewer template string resources
	  parameterEditorSelectNone: "clear selection",
	  parameterEditorSelectAll: "select all",
	  parametersAreaPreviewButton: "Preview",
	  menuNavigateBackwardText: "Navigate Backward",
	  menuNavigateBackwardTitle: "Navigate Backward",
	  menuNavigateForwardText: "Navigate Forward",
	  menuNavigateForwardTitle: "Navigate Forward",
	  menuStopRenderingText: "Stop Rendering",
	  menuStopRenderingTitle: "Stop Rendering",
	  menuRefreshText: "Refresh",
	  menuRefreshTitle: "Refresh",
	  menuFirstPageText: "First Page",
	  menuFirstPageTitle: "First Page",
	  menuLastPageText: "Last Page",
	  menuLastPageTitle: "Last Page",
	  menuPreviousPageTitle: "Previous Page",
	  menuNextPageTitle: "Next Page",
	  menuPageNumberTitle: "Page Number Selector",
	  menuPageText: "Page",
	  menuPageOfText: "of",
	  menuDocumentMapTitle: "Toggle Document Map",
	  menuParametersAreaTitle: "Toggle Parameters Area",
	  menuZoomInTitle: "Zoom In",
	  menuZoomOutTitle: "Zoom Out",
	  menuPageStateTitle: "Toggle FullPage/PageWidth",
	  menuPrintText: "Print...",
	  menuContinuousScrollText: "Toggle Continuous Scrolling",
	  menuSendMailText: "Send an email",
	  menuPrintTitle: "Print",
	  menuContinuousScrollTitle: "Toggle Continuous Scrolling",
	  menuSendMailTitle: "Send an email",
	  menuExportText: "Export",
	  menuExportTitle: "Export",
	  menuPrintPreviewText: "Toggle Print Preview",
	  menuPrintPreviewTitle: "Toggle Print Preview",
	  menuSearchText: "Search",
	  menuSearchTitle: "Toggle Search",
	  menuAiPromptTitle: "Toggle AI Prompt",
	  menuSideMenuTitle: "Toggle Side Menu",
	  sendEmailFromLabel: "From:",
	  sendEmailToLabel: "To:",
	  sendEmailCCLabel: "CC:",
	  sendEmailSubjectLabel: "Subject:",
	  sendEmailFormatLabel: "Format:",
	  sendEmailSendLabel: "Send",
	  sendEmailCancelLabel: "Cancel",
	  // accessibility string resources
	  ariaLabelPageNumberSelector: "Page number selector. Showing page {0} of {1}.",
	  ariaLabelPageNumberEditor: "Page number editor",
	  ariaLabelExpandable: "Expandable",
	  ariaLabelSelected: "Selected",
	  ariaLabelParameter: "parameter",
	  ariaLabelErrorMessage: "Error message",
	  ariaLabelParameterInfo: "Contains {0} options",
	  ariaLabelMultiSelect: "Multiselect",
	  ariaLabelMultiValue: "Multivalue",
	  ariaLabelSingleValue: "Single value",
	  ariaLabelParameterDateTime: "DateTime",
	  ariaLabelParameterString: "String",
	  ariaLabelParameterNumerical: "Numerical",
	  ariaLabelParameterBoolean: "Boolean",
	  ariaLabelParametersAreaPreviewButton: "Preview the report",
	  ariaLabelMainMenu: "Main menu",
	  ariaLabelCompactMenu: "Compact menu",
	  ariaLabelSideMenu: "Side menu",
	  ariaLabelDocumentMap: "Document map area",
	  ariaLabelDocumentMapSplitter: "Document map area splitbar.",
	  ariaLabelParametersAreaSplitter: "Parameters area splitbar.",
	  ariaLabelPagesArea: "Report contents area",
	  ariaLabelSearchDialogArea: "Search area",
	  ariaLabelAiPromptDialogArea: "AI prompt area",
	  ariaLabelSendEmailDialogArea: "Send email area",
	  ariaLabelSearchDialogStop: "Stop search",
	  ariaLabelSearchDialogOptions: "Search options",
	  ariaLabelSearchDialogNavigateUp: "Navigate up",
	  ariaLabelSearchDialogNavigateDown: "Navigate down",
	  ariaLabelSearchDialogMatchCase: "Match case",
	  ariaLabelSearchDialogMatchWholeWord: "Match whole word",
	  ariaLabelSearchDialogUseRegex: "Use regex",
	  ariaLabelMenuNavigateBackward: "Navigate backward",
	  ariaLabelMenuNavigateForward: "Navigate forward",
	  ariaLabelMenuStopRendering: "Stop rendering",
	  ariaLabelMenuRefresh: "Refresh",
	  ariaLabelMenuFirstPage: "First page",
	  ariaLabelMenuLastPage: "Last page",
	  ariaLabelMenuPreviousPage: "Previous page",
	  ariaLabelMenuNextPage: "Next page",
	  ariaLabelMenuPageNumber: "Page number selector",
	  ariaLabelMenuDocumentMap: "Toggle document map",
	  ariaLabelMenuParametersArea: "Toggle parameters area",
	  ariaLabelMenuZoomIn: "Zoom in",
	  ariaLabelMenuZoomOut: "Zoom out",
	  ariaLabelMenuPageState: "Toggle FullPage/PageWidth",
	  ariaLabelMenuPrint: "Print",
	  ariaLabelMenuContinuousScroll: "Continuous scrolling",
	  ariaLabelMenuSendMail: "Send an email",
	  ariaLabelMenuExport: "Export",
	  ariaLabelMenuPrintPreview: "Toggle print preview",
	  ariaLabelMenuSearch: "Search in report contents",
	  ariaLabelMenuSideMenu: "Toggle side menu",
	  ariaLabelSendEmailFrom: "From email address",
	  ariaLabelSendEmailTo: "Recipient email address",
	  ariaLabelSendEmailCC: "Carbon Copy email address",
	  ariaLabelSendEmailSubject: "Email subject:",
	  ariaLabelSendEmailFormat: "Report format:",
	  ariaLabelSendEmailSend: "Send email",
	  ariaLabelSendEmailCancel: "Cancel sending email",
	  // search dialog resources
	  searchDialogTitle: "Search in report contents",
	  searchDialogSearchInProgress: "searching...",
	  searchDialogNoResultsLabel: "No results",
	  searchDialogResultsFormatLabel: "Result {0} of {1}",
	  searchDialogStopTitle: "Stop Search",
	  searchDialogNavigateUpTitle: "Navigate Up",
	  searchDialogNavigateDownTitle: "Navigate Down",
	  searchDialogMatchCaseTitle: "Match Case",
	  searchDialogMatchWholeWordTitle: "Match Whole Word",
	  searchDialogUseRegexTitle: "Use Regex",
	  searchDialogCaptionText: "Find",
	  searchDialogPageText: "page",
	  // Send Email dialog resources
	  sendEmailDialogTitle: "Send Email",
	  sendEmailValidationEmailRequired: "Email field is required",
	  sendEmailValidationEmailFormat: "Email format is not valid",
	  sendEmailValidationSingleEmail: "The field accepts a single email address only",
	  sendEmailValidationFormatRequired: "Format field is required",
	  errorSendingDocument: "Error sending report document (Report = '{0}').",
	  aiPromptDialogConsentTitle: "Before you start with AI",
	  aiPromptDialogConsentAccept: "Consent",
	  aiPromptDialogConsentReject: "Cancel",
	  aiPromptDialogTextAreaPlaceholder: "Enter your prompt",
	  aiPromptDialogNoPredefinedAndCustomPromptsPlaceholder: "Custom prompts are disabled and there are no predefined prompts configured. Please allow custom prompts or add predefined prompts to use the AI feature.",
	  aiPromptDialogNoCustomPromptsPlaceholder: "Custom prompts are disabled, please select one of the predefined suggestions below"
	};
	window.telerikReportViewer ||= {};
	window.telerikReportViewer.sr ||= sr$1;

	var sr = sr || {};
	const userResources = (window.telerikReportViewer || {}).sr || {};
	const stringResources = $.extend({}, sr, userResources);

	var parameterEditorsMatch = {
	  // AvailableValues PROVIDED, MultiValue is TRUE and trv.parameters.editors.multiSelect is unset
	  MultiSelect: (parameter, editorsType) => {
	    return Boolean(parameter.availableValues) && parameter.multivalue && (!editorsType || !editorsType.multiSelect || editorsType.multiSelect !== ParameterEditorTypes.COMBO_BOX);
	  },
	  // AvailableValues PROVIDED, MultiValue is TRUE and trv.parameters.editors.multiSelect is set to COMBO_BOX
	  MultiSelectCombo: (parameter, editorsType) => {
	    return Boolean(parameter.availableValues) && parameter.multivalue && (editorsType && editorsType.multiSelect && editorsType.multiSelect === ParameterEditorTypes.COMBO_BOX);
	  },
	  // AvailableValues PROVIDED, MultiValue is FALSE and trv.parameters.editors.singleSelect is unset
	  SingleSelect: (parameter, editorsType) => {
	    return Boolean(parameter.availableValues) && !parameter.multivalue && (!editorsType || !editorsType.singleSelect || editorsType.singleSelect !== ParameterEditorTypes.COMBO_BOX);
	  },
	  // AvailableValues PROVIDED, MultiValue is FALSE and trv.parameters.editors.singleSelect is set to COMBO_BOX
	  SingleSelectCombo: (parameter, editorsType) => {
	    return Boolean(parameter.availableValues) && !parameter.multivalue && (editorsType && editorsType.singleSelect && editorsType.singleSelect === ParameterEditorTypes.COMBO_BOX);
	  },
	  // AvailableValues NOT PROVIDED and MultiValue is TRUE
	  MultiValue: (parameter) => {
	    return Boolean(parameter.multivalue);
	  },
	  DateTime: (parameter) => {
	    return parameter.type === ParameterTypes.DATETIME;
	  },
	  String: (parameter) => {
	    return parameter.type === ParameterTypes.STRING;
	  },
	  Number: (parameter) => {
	    switch (parameter.type) {
	      case ParameterTypes.INTEGER:
	      case ParameterTypes.FLOAT:
	        return true;
	      default:
	        return false;
	    }
	  },
	  Boolean: (parameter) => {
	    return parameter.type === ParameterTypes.BOOLEAN;
	  },
	  Default: (parameter) => {
	    return true;
	  }
	};
	var multivalueUtils = function() {
	  var lineSeparator = "\n";
	  return {
	    formatValue: (value) => {
	      var text = "";
	      if (value) {
	        [].concat(value).forEach((val) => {
	          if (text.length > 0) {
	            text += lineSeparator;
	          }
	          text += val;
	        });
	      }
	      return text;
	    },
	    parseValues: (text) => {
	      return ("" + text).split(lineSeparator);
	    }
	  };
	}();
	function integerInputBehavior(input) {
	  function isValid(newValue) {
	    return /^(\-|\+)?([0-9]*)$/.test(newValue);
	  }
	  function onKeyPress(event) {
	    if (isSpecialKey(event.keyCode)) {
	      return true;
	    }
	    return isValid($(input).val() + String.fromCharCode(event.charCode));
	  }
	  function onPaste(event) {
	  }
	  function attach(input2) {
	    $(input2).on("keypress", onKeyPress).on("paste", onPaste);
	  }
	  function detach(input2) {
	    $(input2).off("keypress", onKeyPress).off("paste", onPaste);
	  }
	  attach(input);
	  return {
	    dispose: () => {
	      detach(input);
	    }
	  };
	}
	function floatInputBehavior(input) {
	  function isValid(newValue) {
	    return /^(\-|\+)?([0-9]*(\.[0-9]*)?)$/.test(newValue);
	  }
	  function onKeyPress(event) {
	    if (isSpecialKey(event.keyCode)) {
	      return true;
	    }
	    return isValid($(input).val() + String.fromCharCode(event.charCode));
	  }
	  function onPaste(event) {
	  }
	  function attach(input2) {
	    $(input2).on("keypress", onKeyPress).on("paste", onPaste);
	  }
	  function detach(input2) {
	    $(input2).off("keypress", onKeyPress).off("paste", onPaste);
	  }
	  attach(input);
	  return {
	    dispose: () => {
	      detach(input);
	    }
	  };
	}
	function applyClass(apply, cssClass, item) {
	  var fn = apply ? $.fn.addClass : $.fn.removeClass;
	  fn.call(item, cssClass);
	}
	function enableItem(item, enable) {
	  applyClass(!enable, "k-disabled", item);
	}
	function selectItem(item, select) {
	  applyClass(select, "k-selected", item);
	  item.attr("aria-selected", select);
	}
	function addAccessibilityAttributes(editor, type, caption, additionalInfo, error) {
	  if (!additionalInfo) {
	    additionalInfo = "";
	  }
	  var label = stringFormat("{0}. {1} {2}. {3}", [caption, type, stringResources.ariaLabelParameter, additionalInfo]);
	  editor.attr("aria-label", label);
	  setAccessibilityErrorAttributes(editor, error);
	}
	var containerTabIndex;
	var editorsIndex = 0;
	function setEditorTabIndex(editor) {
	  if (!containerTabIndex) {
	    var $container = $(".trv-parameters-area-content");
	    if ($container.length > 0) {
	      var tabIndexAttr = $container.attr("tabIndex");
	      if (tabIndexAttr) {
	        containerTabIndex = distExports.tryParseInt(tabIndexAttr);
	      }
	    }
	    if (!containerTabIndex || isNaN(containerTabIndex)) {
	      containerTabIndex = 300;
	    }
	  }
	  var wrapper = editor.closest(".trv-parameter-value");
	  var selectAll = wrapper.find(".trv-select-all");
	  var clearSelection = wrapper.find(".trv-select-none");
	  var widgetParent = editor.closest(".k-widget");
	  var hasFocusableElement = widgetParent.find(".k-input");
	  var isComboWidget = hasFocusableElement && hasFocusableElement.length;
	  if (selectAll && selectAll.length) {
	    selectAll.attr("tabindex", containerTabIndex + ++editorsIndex);
	  }
	  if (clearSelection && clearSelection.length) {
	    clearSelection.attr("tabindex", containerTabIndex + ++editorsIndex);
	  }
	  if (isComboWidget) {
	    hasFocusableElement.attr("tabindex", containerTabIndex + ++editorsIndex);
	  } else {
	    editor.attr("tabindex", containerTabIndex + ++editorsIndex);
	  }
	}
	function setAccessibilityErrorAttributes(editor, error) {
	  var errToken = stringFormat(" {0}:", [stringResources.ariaLabelErrorMessage]);
	  var label = editor.attr("aria-label");
	  if (!label) {
	    return;
	  }
	  var errIdx = label.indexOf(errToken);
	  if (errIdx > -1) {
	    label = label.substring(0, errIdx);
	  }
	  if (error && error !== "") {
	    editor.attr("aria-required", true);
	    editor.attr("aria-invalid", true);
	    label += errToken + error;
	  } else {
	    editor.removeAttr("aria-invalid");
	  }
	  editor.attr("aria-label", label);
	}
	function navigatableEnabledForList(enableAccessibility) {
	  return kendo.version >= "2017.3.1018" || enableAccessibility;
	}
	var ParameterEditors = [
	  {
	    match: parameterEditorsMatch.MultiSelect,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var enabled = true;
	      $placeholder.html(options.templates["trv-parameter-editor-available-values-multiselect"]);
	      var $list = $placeholder.find(".trv-list");
	      var $selectAll = $placeholder.find(".trv-select-all");
	      var $selectNone = $placeholder.find(".trv-select-none");
	      var listView;
	      var parameter;
	      var updateTimeout;
	      var valueChangeCallback = options.parameterChanged;
	      var initialized;
	      if ($selectAll.length > 0) {
	        $selectAll.text(stringResources[$selectAll.text()]);
	        $selectAll = new kendo.ui.Button($selectAll, {
	          size: "small",
	          click: (e) => {
	            e.preventDefault();
	            if (!enabled) {
	              return;
	            }
	            setSelectedItems(parameter.availableValues.map((av) => {
	              return av.value;
	            }));
	          }
	        });
	      }
	      if ($selectNone.length > 0) {
	        $selectNone.text(stringResources[$selectNone.text()]);
	        $selectNone = new kendo.ui.Button($selectNone, {
	          size: "small",
	          click: (e) => {
	            e.preventDefault();
	            if (!enabled)
	              return;
	            setSelectedItems([]);
	          }
	        });
	      }
	      function onSelectionChanged(selection) {
	        if (initialized) {
	          applyAriaSelected(selection);
	          notifyParameterChanged(selection);
	        }
	      }
	      function applyAriaSelected(selection) {
	        var children = $list.find(".trv-listviewitem");
	        Array.from(children).forEach((item) => {
	          var $item = $(item);
	          var isSelected = selection.filter($item).length > 0;
	          $item.attr("aria-selected", isSelected);
	        });
	      }
	      function notifyParameterChanged(selection) {
	        var availableValues = parameter.availableValues;
	        var values = $.map(selection, (item) => {
	          return availableValues[$(item).index()].value;
	        });
	        clearPendingChange();
	        var immediateUpdate = !parameter.autoRefresh && !parameter.childParameters;
	        updateTimeout = window.setTimeout(() => {
	          if (!areEqualArrays(parameter.value, values)) {
	            valueChangeCallback(parameter, values);
	          }
	          updateTimeout = null;
	        }, immediateUpdate ? 0 : 1e3);
	      }
	      function clearPendingChange() {
	        if (updateTimeout) {
	          window.clearTimeout(updateTimeout);
	        }
	      }
	      function getSelectedItems() {
	        return $(listView.element).find(".k-selected");
	      }
	      function onItemClick(e) {
	        if (!enabled)
	          return;
	        var clickedItem = $(e.target);
	        var selectedItems = listView.select();
	        if (clickedItem.hasClass("k-selected")) {
	          selectedItems.splice($.inArray(clickedItem[0], selectedItems), 1);
	        } else {
	          selectedItems.push(clickedItem);
	        }
	        listView.clearSelection();
	        listView.select(selectedItems);
	        listView.trigger("change");
	      }
	      function onChange(e) {
	        onSelectionChanged(getSelectedItems());
	      }
	      function onKeydown(event) {
	        if (!enabled)
	          return;
	        if (event.which !== 32) {
	          return;
	        }
	        var focused = listView.element.find(".k-focus");
	        if (focused.length > 0) {
	          focused.toggleClass("k-selected");
	          onSelectionChanged(getSelectedItems());
	          event.preventDefault();
	        }
	      }
	      function init($list2) {
	        setEditorTabIndex($list2);
	        setSelectedItems(parameter.value);
	        listView.element.off().on("touch click", ".trv-listviewitem", onItemClick);
	        listView.element.on("keydown", onKeydown);
	        initialized = true;
	      }
	      function clear() {
	        initialized = false;
	        if (listView) {
	          listView.element.off("touch click", ".trv-listviewitem", onItemClick);
	          listView.element.off("keydown", onKeydown);
	        }
	      }
	      function setSelectedItems(items) {
	        setSelectedItemsCore(items);
	        onSelectionChanged(getSelectedItems());
	      }
	      function setSelectedItemsCore(items) {
	        if (!Array.isArray(items)) {
	          items = [items];
	        }
	        var children = $list.find(".trv-listviewitem");
	        $.each(parameter.availableValues, (i, av) => {
	          var selected = false;
	          $.each(items, (j, v) => {
	            var availableValue = av.value;
	            if (v instanceof Date) {
	              availableValue = distExports.parseToLocalDate(av.value);
	            }
	            selected = areEqual(v, availableValue);
	            return !selected;
	          });
	          selectItem($(children[i]), selected);
	        });
	      }
	      return {
	        beginEdit: (param) => {
	          clear();
	          parameter = param;
	          try {
	            listView = $list.kendoListView({
	              template: kendo.template('<div class="trv-listviewitem" style="cursor: pointer">${name}</div>'),
	              dataSource: { data: parameter.availableValues },
	              selectable: "MULTIPLE",
	              navigatable: navigatableEnabledForList(options.enableAccessibility),
	              change: onChange
	            }).data("kendoListView");
	          } catch (e) {
	            console.error("Instantiation of Kendo ListView as MultiSelect parameter editor threw an exception", e);
	            throw e;
	          }
	          init($list);
	        },
	        enable: (enable) => {
	          enabled = enable;
	          enableItem($list, enabled);
	        },
	        clearPendingChange,
	        addAccessibility: (param) => {
	          var info = stringFormat(stringResources.ariaLabelParameterInfo, [param.availableValues.length]);
	          addAccessibilityAttributes($list, stringResources.ariaLabelMultiSelect, param.text, info, param.Error);
	          $list.attr("aria-multiselectable", "true");
	          var items = $list.find(".trv-listviewitem");
	          Array.from(items).forEach((item) => {
	            $(item).attr("aria-label", item.innerText);
	          });
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($list, param.Error);
	        },
	        destroy: () => {
	          listView.destroy();
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.MultiSelectCombo,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var enabled = true;
	      var selector = ".trv-combo";
	      var template = "trv-parameter-editor-available-values-multiselect-combo";
	      var valueChangeCallback = options.parameterChanged;
	      var $editorDom;
	      var $selectNone;
	      var $selectAll;
	      var editor;
	      var updateTimeout;
	      var popUpIsClosed = true;
	      var parameter;
	      $placeholder.html(options.templates[template]);
	      $editorDom = $placeholder.find(selector);
	      $selectAll = $placeholder.find(".trv-select-all");
	      if ($selectAll.length > 0) {
	        $selectAll.text(stringResources[$selectAll.text()]);
	        $selectAll = new kendo.ui.Button($selectAll, {
	          size: "small",
	          click: (e) => {
	            e.preventDefault();
	            if (!enabled) {
	              return;
	            }
	            var values = $.map(parameter.availableValues, (dataItem) => {
	              return dataItem.value;
	            });
	            editor.value(values);
	            editor.trigger("change");
	          }
	        });
	      }
	      $selectNone = $placeholder.find(".trv-select-none");
	      if ($selectNone.length > 0) {
	        $selectNone.text(stringResources[$selectNone.text()]);
	        $selectNone = new kendo.ui.Button($selectNone, {
	          size: "small",
	          click: (e) => {
	            e.preventDefault();
	            editor.value([]);
	            editor.trigger("change");
	          }
	        });
	      }
	      function onSelectionChanged(selection) {
	        notifyParameterChanged(selection);
	      }
	      function notifyParameterChanged(values) {
	        clearPendingChange();
	        var immediateUpdate = !parameter.autoRefresh && !parameter.childParameters;
	        updateTimeout = window.setTimeout(() => {
	          if (!areEqualArrays(parameter.value, values)) {
	            valueChangeCallback(parameter, values);
	          }
	          updateTimeout = null;
	        }, immediateUpdate ? 0 : 1e3);
	      }
	      function clearPendingChange() {
	        if (updateTimeout) {
	          window.clearTimeout(updateTimeout);
	        }
	      }
	      function getSelectedItems() {
	        return editor.value();
	      }
	      function onChange() {
	        if (popUpIsClosed) {
	          onSelectionChanged(getSelectedItems());
	        }
	      }
	      function init($editorDom2) {
	        setEditorTabIndex($editorDom2);
	        editor.bind("change", onChange);
	      }
	      function reset() {
	        if (editor) {
	          editor.unbind("change", onChange);
	        }
	      }
	      return {
	        beginEdit: (param) => {
	          reset();
	          parameter = param;
	          try {
	            editor = $editorDom.kendoMultiSelect({
	              itemTemplate: '<div class="trv-editoritem">${name}</div>',
	              dataSource: parameter.availableValues,
	              dataTextField: "name",
	              dataValueField: "value",
	              value: parameter.value,
	              filter: "contains",
	              autoClose: false,
	              open: () => {
	                popUpIsClosed = false;
	              },
	              close: (e) => {
	                popUpIsClosed = true;
	                onChange();
	              },
	              autoWidth: true,
	              clearButton: false
	            }).data("kendoMultiSelect");
	          } catch (e) {
	            console.error("Instantiation of Kendo MultiSelect as MultiSelectCombo parameter editor threw an exception", e);
	            throw e;
	          }
	          init($editorDom);
	        },
	        enable: (enable) => {
	          enabled = enable;
	          editor.enable(enable);
	        },
	        clearPendingChange,
	        addAccessibility: (param) => {
	          var $accessibilityDom = editor.input;
	          var info = stringFormat(stringResources.ariaLabelParameterInfo, [param.availableValues.length]);
	          addAccessibilityAttributes($accessibilityDom, stringResources.ariaLabelMultiSelect, param.text, info, param.Error);
	          var items = editor.items();
	          Array.from(items).forEach((item) => {
	            $(item).attr("aria-label", item.innerText);
	          });
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($editorDom, param.Error);
	        },
	        destroy: () => {
	          editor.destroy();
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.SingleSelect,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var enabled = true;
	      $placeholder.html(options.templates["trv-parameter-editor-available-values"]);
	      var $list = $placeholder.find(".trv-list");
	      var $selectNone = $placeholder.find(".trv-select-none");
	      var listView;
	      var parameter;
	      var valueChangeCallback = options.parameterChanged;
	      if ($selectNone.length > 0) {
	        $selectNone.text(stringResources[$selectNone.text()]);
	        $selectNone = new kendo.ui.Button($selectNone, {
	          size: "small",
	          click: (e) => {
	            e.preventDefault();
	            listView.clearSelection();
	            listView.trigger("change");
	          }
	        });
	      }
	      function onSelectionChanged(selection) {
	        notifyParameterChanged(selection);
	      }
	      function notifyParameterChanged(selection) {
	        var availableValues = parameter.availableValues;
	        var values = $.map(selection, (item) => {
	          return availableValues[$(item).index()].value;
	        });
	        if (Array.isArray(values)) {
	          values = values[0];
	        }
	        valueChangeCallback(parameter, values);
	      }
	      function getSelectedItems() {
	        return listView.select();
	      }
	      function onChange() {
	        onSelectionChanged(getSelectedItems());
	      }
	      function init($list2) {
	        setEditorTabIndex($list2);
	        setSelectedItems(parameter.value);
	        listView.bind("change", onChange);
	      }
	      function reset() {
	        if (listView) {
	          listView.unbind("change", onChange);
	        }
	      }
	      function setSelectedItems(value) {
	        var items = $list.find(".trv-listviewitem");
	        $.each(parameter.availableValues, (i, av) => {
	          var availableValue = av.value;
	          if (value instanceof Date) {
	            availableValue = distExports.parseToLocalDate(av.value);
	          }
	          if (areEqual(value, availableValue)) {
	            listView.select(items[i]);
	            return false;
	          }
	          return true;
	        });
	      }
	      return {
	        beginEdit: (param) => {
	          reset();
	          parameter = param;
	          try {
	            listView = $list.kendoListView({
	              template: '<div class="trv-listviewitem">${name}</div>',
	              dataSource: { data: parameter.availableValues },
	              selectable: true,
	              navigatable: navigatableEnabledForList(options.enableAccessibility)
	            }).data("kendoListView");
	          } catch (e) {
	            console.error("Instantiation of Kendo ListView as SingleSelect parameter editor threw an exception", e);
	            throw e;
	          }
	          init($list);
	        },
	        enable: (enable) => {
	          enabled = enable;
	          enableItem($list, enabled);
	          if (enabled) {
	            listView.bind("change", onChange);
	            $list.addClass("k-selectable");
	          } else {
	            listView.unbind("change", onChange);
	            $list.removeClass("k-selectable");
	          }
	        },
	        addAccessibility: (param) => {
	          var info = stringFormat(stringResources.ariaLabelParameterInfo, [param.availableValues.length]);
	          addAccessibilityAttributes($list, stringResources.ariaLabelSingleValue, param.text, info, param.Error);
	          var items = $list.find(".trv-listviewitem");
	          Array.from(items).forEach((item) => {
	            $(item).attr("aria-label", item.innerText);
	          });
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($list, param.Error);
	        },
	        destroy: () => {
	          listView.destroy();
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.SingleSelectCombo,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var selector = ".trv-combo";
	      var template = "trv-parameter-editor-available-values-combo";
	      var valueChangeCallback = options.parameterChanged;
	      var $editorDom;
	      var $selectNone;
	      var editor;
	      var parameter;
	      $placeholder.html(options.templates[template]);
	      $editorDom = $placeholder.find(selector);
	      $selectNone = $placeholder.find(".trv-select-none");
	      if ($selectNone.length > 0) {
	        $selectNone.text(stringResources[$selectNone.text()]);
	        $selectNone = new kendo.ui.Button($selectNone, {
	          size: "small",
	          click: (e) => {
	            e.preventDefault();
	            editor.value("");
	            editor.trigger("change");
	          }
	        });
	      }
	      function onSelectionChanged(selection, value) {
	        notifyParameterChanged(selection, value);
	      }
	      function notifyParameterChanged(selection, value) {
	        var values = value || "";
	        var availableValues;
	        if (!value && selection >= 0) {
	          availableValues = parameter.availableValues;
	          values = availableValues[selection].value;
	        }
	        valueChangeCallback(parameter, values);
	      }
	      function getSelectedItems() {
	        return editor.select();
	      }
	      function onChange(event) {
	        onSelectionChanged(getSelectedItems(), event.sender.value());
	      }
	      function init($editorDom2) {
	        setEditorTabIndex($editorDom2);
	        editor.bind("change", onChange);
	      }
	      function reset() {
	        if (editor) {
	          editor.unbind("change", onChange);
	        }
	      }
	      return {
	        beginEdit: (param) => {
	          reset();
	          parameter = param;
	          try {
	            editor = $editorDom.kendoComboBox({
	              template: '<div class="trv-editoritem">${name}</div>',
	              dataSource: parameter.availableValues,
	              dataTextField: "name",
	              dataValueField: "value",
	              value: parameter.value,
	              filter: "contains",
	              suggest: true,
	              clearButton: false
	            }).data("kendoComboBox");
	          } catch (e) {
	            console.error("Instantiation of Kendo ComboBox as SingleSelectCombo parameter editor threw an exception", e);
	            throw e;
	          }
	          init($editorDom);
	        },
	        enable: (enable) => {
	          editor.enable(enable);
	        },
	        addAccessibility: (param) => {
	          var $accessibilityDom = editor.input;
	          var info = stringFormat(stringResources.ariaLabelParameterInfo, [param.availableValues.length]);
	          addAccessibilityAttributes($accessibilityDom, stringResources.ariaLabelSingleValue, param.text, info, param.Error);
	          var items = editor.items();
	          Array.from(items).forEach((item) => {
	            $(item).attr("aria-label", item.innerText);
	          });
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($editorDom, param.Error);
	        },
	        destroy: () => {
	          editor.destroy();
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.MultiValue,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var parameter;
	      $placeholder.html(options.templates["trv-parameter-editor-multivalue"]);
	      var $textArea = $placeholder.find("textarea").on("change", (event) => {
	        if (options.parameterChanged) {
	          options.parameterChanged(parameter, multivalueUtils.parseValues(event.currentTarget.value));
	        }
	      });
	      function setValue(value) {
	        parameter.value = value;
	        $textArea.val(multivalueUtils.formatValue(value));
	      }
	      return {
	        beginEdit: (param) => {
	          parameter = param;
	          setValue(param.value);
	          setEditorTabIndex($textArea);
	        },
	        enable: (enable) => {
	          enableItem($textArea, enable);
	          $textArea.prop("disabled", !enable);
	        },
	        addAccessibility: (param) => {
	          addAccessibilityAttributes($textArea, stringResources.ariaLabelMultiValue, param.text, null, param.Error);
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($textArea, param.Error);
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.DateTime,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var parameter;
	      $placeholder.html(options.templates["trv-parameter-editor-datetime"]);
	      try {
	        var $dateTimePicker = $placeholder.find("input[type=datetime]").kendoDatePicker({
	          change: (event) => {
	            var handler = options.parameterChanged;
	            if (handler) {
	              var dtv = event.sender.value();
	              if (null !== dtv) {
	                dtv = adjustTimezone(dtv);
	              }
	              handler(parameter, dtv);
	            }
	          }
	        });
	        var dateTimePicker = $dateTimePicker.data("kendoDatePicker");
	      } catch (e) {
	        console.error("Instantiation of Kendo DatePicker as DateTime parameter editor threw an exception", e);
	        throw e;
	      }
	      function setValue(value) {
	        parameter.value = value;
	        var dt = null;
	        try {
	          if (value) {
	            dt = unadjustTimezone(value);
	          }
	        } catch (e) {
	          dt = null;
	        }
	        dateTimePicker.value(dt);
	      }
	      return {
	        beginEdit: (param) => {
	          parameter = param;
	          setValue(param.value);
	          setEditorTabIndex($dateTimePicker);
	        },
	        enable: (enable) => {
	          dateTimePicker.enable(enable);
	          enableItem($dateTimePicker, enable);
	        },
	        addAccessibility: (param) => {
	          addAccessibilityAttributes($dateTimePicker, stringResources.ariaLabelParameterDateTime, param.text, null, param.Error);
	          $dateTimePicker.attr("aria-live", "assertive");
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($dateTimePicker, param.Error);
	        },
	        destroy: () => {
	          dateTimePicker.destroy();
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.String,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var parameter;
	      $placeholder.html(options.templates["trv-parameter-editor-text"]);
	      var $input = $placeholder.find('input[type="text"]').change(() => {
	        if (options.parameterChanged) {
	          options.parameterChanged(parameter, $input.val());
	        }
	      });
	      function setValue(value) {
	        parameter.value = value;
	        $input.val(value);
	      }
	      return {
	        beginEdit: (param) => {
	          parameter = param;
	          setValue(param.value);
	          setEditorTabIndex($input);
	        },
	        enable: (enabled) => {
	          $input.prop("disabled", !enabled);
	          enableItem($input, enabled);
	        },
	        addAccessibility: (param) => {
	          addAccessibilityAttributes($input, stringResources.ariaLabelParameterString, param.text, null, param.Error);
	          $input.attr("aria-live", "assertive");
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($input, param.Error);
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.Number,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var parameter;
	      var inputBehavior;
	      $placeholder.html(options.templates["trv-parameter-editor-number"]);
	      var $input = $placeholder.find("input[type=number]").on("change", () => {
	        if (options.parameterChanged) {
	          options.parameterChanged(parameter, $input.val());
	        }
	      });
	      return {
	        beginEdit: (param) => {
	          if (inputBehavior) {
	            inputBehavior.dispose();
	          }
	          parameter = param;
	          $input.val(parameter.value);
	          if (parameter.type === ParameterTypes.INTEGER) {
	            inputBehavior = integerInputBehavior($input);
	          } else {
	            inputBehavior = floatInputBehavior($input);
	          }
	          setEditorTabIndex($input);
	        },
	        enable: (enable) => {
	          $input.prop("disabled", !enable);
	          enableItem($input, enable);
	        },
	        addAccessibility: (param) => {
	          addAccessibilityAttributes($input, stringResources.ariaLabelParameterNumerical, param.text, null, param.Error);
	          $input.attr("aria-live", "assertive");
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($input, param.Error);
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.Boolean,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      var parameter;
	      $placeholder.html(options.templates["trv-parameter-editor-boolean"]);
	      var $input = $placeholder.find("input[type=checkbox]").on("change", (event) => {
	        if (options.parameterChanged) {
	          options.parameterChanged(parameter, event.currentTarget.checked);
	        }
	      });
	      function setValue(value) {
	        parameter.value = value;
	        $input[0].checked = value === true;
	      }
	      return {
	        beginEdit: (param) => {
	          parameter = param;
	          setValue(param.value);
	          setEditorTabIndex($input);
	        },
	        enable: (enable) => {
	          enableItem($input, enable);
	          $input.attr("disabled", !enable);
	        },
	        addAccessibility: (param) => {
	          addAccessibilityAttributes($input, stringResources.ariaLabelParameterBoolean, param.text, null, param.Error);
	          $input.attr("aria-live", "assertive");
	        },
	        setAccessibilityErrorState: (param) => {
	          setAccessibilityErrorAttributes($input, param.Error);
	        }
	      };
	    }
	  },
	  {
	    match: parameterEditorsMatch.Default,
	    createEditor: (placeholder, options) => {
	      var $placeholder = $(placeholder);
	      $placeholder.html('<div class="trv-parameter-editor-generic"></div>');
	      return {
	        beginEdit: (parameter) => {
	          $placeholder.find(".trv-parameter-editor-generic").text(parameter.Error ? "(error)" : parameter.value);
	        },
	        enable: (enable) => {
	        }
	      };
	    }
	  }
	];

	var ParameterValidators = function() {
	  var validators = {};
	  function validateParameter(parameter, value, validatorFunc, compareFunc) {
	    var values = [].concat(value).map((value1) => {
	      return checkAvailableValues(parameter, validatorFunc(value1), compareFunc);
	    });
	    if (parameter.multivalue) {
	      if (value == null || value.length == 0) {
	        if (parameter.allowNull) {
	          return value;
	        }
	        throw stringResources.invalidParameter;
	      }
	      return values;
	    }
	    return values[0];
	  }
	  function isNull(parameter, value) {
	    return parameter.allowNull && -1 != [null, "", void 0].indexOf(value);
	  }
	  function checkAvailableValues(parameter, value, compareFunc) {
	    if (parameter.availableValues) {
	      var found = Array.from(parameter.availableValues).some((av) => {
	        return compareFunc(value, av.value);
	      });
	      if (!found) {
	        if (parameter.allowNull && !value) {
	          return value;
	        }
	        throw stringResources.invalidParameter;
	      }
	    }
	    return value;
	  }
	  validators[ParameterTypes.STRING] = {
	    validate: (parameter, value) => {
	      return validateParameter(
	        parameter,
	        value,
	        (value2) => {
	          if (!value2) {
	            if (parameter.allowNull) {
	              return null;
	            }
	            if (parameter.allowBlank) {
	              return "";
	            }
	            throw stringResources.parameterIsEmpty;
	          }
	          return value2;
	        },
	        (s1, s2) => {
	          return s1 == s2;
	        }
	      );
	    }
	  };
	  validators[ParameterTypes.FLOAT] = {
	    validate: (parameter, value) => {
	      return validateParameter(
	        parameter,
	        value,
	        (value2) => {
	          var num = distExports.tryParseFloat(value2);
	          if (isNaN(num)) {
	            if (isNull(parameter, value2)) {
	              return null;
	            }
	            throw stringResources.parameterIsEmpty;
	          }
	          return num;
	        },
	        (f1, f2) => {
	          return distExports.tryParseFloat(f1) == distExports.tryParseFloat(f2);
	        }
	      );
	    }
	  };
	  validators[ParameterTypes.INTEGER] = {
	    validate: (parameter, value) => {
	      return validateParameter(
	        parameter,
	        value,
	        (value2) => {
	          var num = distExports.tryParseInt(value2);
	          if (isNaN(num)) {
	            if (isNull(parameter, value2)) {
	              return null;
	            }
	            throw stringResources.parameterIsEmpty;
	          }
	          return num;
	        },
	        (n1, n2) => {
	          return distExports.tryParseInt(n1) == distExports.tryParseFloat(n2);
	        }
	      );
	    }
	  };
	  validators[ParameterTypes.DATETIME] = {
	    validate: (parameter, value) => {
	      return validateParameter(
	        parameter,
	        value,
	        (value2) => {
	          if (parameter.allowNull && (value2 === null || value2 === "" || value2 === void 0)) {
	            return null;
	          }
	          if (!isNaN(Date.parse(value2))) {
	            if (parameter.availableValues) {
	              return value2;
	            }
	            return distExports.parseToLocalDate(value2);
	          }
	          throw stringResources.invalidDateTimeValue;
	        },
	        (d1, d2) => {
	          d1 = distExports.parseToLocalDate(d1);
	          d2 = distExports.parseToLocalDate(d2);
	          return d1.getTime() == d2.getTime();
	        }
	      );
	    }
	  };
	  validators[ParameterTypes.BOOLEAN] = {
	    validate: (parameter, value) => {
	      return validateParameter(
	        parameter,
	        value,
	        (value2) => {
	          if (-1 != ["true", "false"].indexOf(String(value2).toLowerCase())) {
	            return Boolean(value2);
	          }
	          if (isNull(parameter, value2)) {
	            return null;
	          }
	          throw stringResources.parameterIsEmpty;
	        },
	        (b1, b2) => {
	          return Boolean(b1) == Boolean(b2);
	        }
	      );
	    }
	  };
	  return {
	    validate: (parameter, value) => {
	      var v = validators[parameter.type];
	      if (!v) {
	        throw stringFormat(stringResources.cannotValidateType, parameter);
	      }
	      return v.validate(parameter, value);
	    }
	  };
	}();

	function PerspectiveManager(element, controller, notificationService) {
	  var smallMenu = element.querySelectorAll ? element.querySelectorAll(".trv-menu-small")[0] : false;
	  var perspectives = {
	    "small": {
	      documentMapVisible: false,
	      parametersAreaVisible: false,
	      onDocumentMapVisibleChanged: (e, args) => {
	        if (args.visible) {
	          notificationService.setParametersAreaVisible({ visible: false });
	        }
	      },
	      onParameterAreaVisibleChanged: (e, args) => {
	        if (args.visible) {
	          notificationService.setDocumentMapVisible({ visible: false });
	        }
	      },
	      onBeforeLoadReport: () => {
	        notificationService.setParametersAreaVisible({ visible: false });
	        notificationService.setDocumentMapVisible({ visible: false });
	      },
	      onNavigateToPage: () => {
	        notificationService.setParametersAreaVisible({ visible: false });
	        notificationService.setDocumentMapVisible({ visible: false });
	      }
	    },
	    "large": {
	      documentMapVisible: true,
	      parametersAreaVisible: true
	    }
	  };
	  var currentPerspective;
	  function init() {
	    currentPerspective = getPerspective();
	    initStateFromController(perspectives["large"]);
	  }
	  function setPerspective(beforeApplyState) {
	    var perspective = getPerspective();
	    if (perspective !== currentPerspective) {
	      var oldState = perspectives[currentPerspective];
	      var newState = perspectives[perspective];
	      currentPerspective = perspective;
	      if (beforeApplyState) {
	        beforeApplyState.call(void 0, oldState, newState);
	      }
	      applyState(newState);
	    }
	  }
	  function onDocumentMapVisibleChanged(e, args) {
	    dispatch("onDocumentMapVisibleChanged", arguments);
	  }
	  function onParameterAreaVisibleChanged(e, args) {
	    dispatch("onParameterAreaVisibleChanged", arguments);
	  }
	  function onBeforeLoadReport() {
	    dispatch("onBeforeLoadReport", arguments);
	  }
	  function onNavigateToPage() {
	    dispatch("onNavigateToPage", arguments);
	  }
	  function onReportLoadComplete() {
	    dispatch("onReportLoadComplete", arguments);
	  }
	  function onWindowResize() {
	    setPerspective((oldState, newState) => {
	      initStateFromController(oldState);
	    });
	  }
	  function onCssLoaded() {
	    setPerspective(null);
	  }
	  function dispatch(func, args) {
	    var activePerspective = perspectives[currentPerspective];
	    var handler = activePerspective[func];
	    if (typeof handler === "function") {
	      handler.apply(activePerspective, args);
	    }
	  }
	  function attach() {
	    window.addEventListener("resize", onWindowResize);
	    controller.onAsync("beforeLoadReport", async () => onBeforeLoadReport()).on("navigateToPage", onNavigateToPage).onAsync("reportLoadComplete", async () => onReportLoadComplete());
	    notificationService.setDocumentMapVisible(onDocumentMapVisibleChanged);
	    notificationService.setParametersAreaVisible(onParameterAreaVisibleChanged);
	    notificationService.cssLoaded(onCssLoaded);
	  }
	  function dispose() {
	    window.removeEventListener("resize", onWindowResize);
	  }
	  function getPerspective() {
	    var windowWidthInEm = $(window).width() / parseFloat($("body").css("font-size"));
	    var windowMinWidth = 40.5;
	    return smallMenu && windowWidthInEm <= windowMinWidth ? "small" : "large";
	  }
	  function initStateFromController(state) {
	    state.documentMapVisible = documentMapVisible();
	    state.parametersAreaVisible = parametersAreaVisible();
	  }
	  function applyState(state) {
	    documentMapVisible(state.documentMapVisible);
	    parametersAreaVisible(state.parametersAreaVisible);
	  }
	  function documentMapVisible() {
	    if (arguments.length === 0) {
	      var args1 = {};
	      notificationService.getDocumentMapState(args1);
	      return args1.visible;
	    }
	    notificationService.setDocumentMapVisible({
	      visible: Boolean(arguments[0])
	    });
	    return this;
	  }
	  function parametersAreaVisible() {
	    if (arguments.length === 0) {
	      var args1 = {};
	      notificationService.getParametersAreaState(args1);
	      return args1.visible;
	    }
	    notificationService.setParametersAreaVisible({
	      visible: Boolean(arguments[0])
	    });
	    return this;
	  }
	  init();
	  return {
	    attach,
	    dispose
	  };
	}

	function UIController(options) {
	  var stateFlags = {
	    ExportInProgress: 1 << 0,
	    PrintInProgress: 1 << 1,
	    RenderInProgress: 1 << 2
	  };
	  function getState(flags) {
	    return (state & flags) != 0;
	  }
	  function setState(flags, value) {
	    if (value) {
	      state |= flags;
	    } else {
	      state &= ~flags;
	    }
	  }
	  var controller = options.controller;
	  var notificationService = options.notificationService;
	  var historyManager = options.history;
	  var state = 0;
	  var refreshUI;
	  var commands = options.commands;
	  if (!controller) {
	    throw "No controller (telerikReporting.ReportViewerController) has been specified.";
	  }
	  if (!notificationService) {
	    throw "No notificationService (telerikReporting.NotificationService) has been specified.";
	  }
	  function getDocumentMapState() {
	    var args = {};
	    notificationService.getDocumentMapState(args);
	    return args;
	  }
	  function getParametersAreaState() {
	    var args = {};
	    notificationService.getParametersAreaState(args);
	    return args;
	  }
	  function getSendEmailDialogState() {
	    var args = {};
	    notificationService.getSendEmailDialogState(args);
	    return args;
	  }
	  function updateUI() {
	    if (!refreshUI) {
	      refreshUI = true;
	      window.setTimeout(() => {
	        try {
	          updateUICore();
	        } finally {
	          refreshUI = false;
	        }
	      }, 10);
	    }
	  }
	  function updateUICore() {
	    var rs = controller.getReportSource();
	    var pageCount = controller.getPageCount();
	    var currentPageNumber = controller.getCurrentPageNumber();
	    var hasReport = rs && rs.report;
	    var hasPages = hasReport && pageCount > 0;
	    var nextPage = hasPages && currentPageNumber < pageCount;
	    var prevPage = hasPages && currentPageNumber > 1;
	    var hasPage = hasPages && currentPageNumber;
	    var documentMapState = getDocumentMapState();
	    var parametersAreaState = getParametersAreaState();
	    var sendEmailDialogState = getSendEmailDialogState();
	    var searchInitiated = controller.getSearchInitiated();
	    var aiPromptInitiated = controller.getAiPromptInitiated();
	    var renderInProgress = getState(stateFlags.RenderInProgress);
	    var printInProgress = getState(stateFlags.PrintInProgress);
	    var exportInProgress = getState(stateFlags.ExportInProgress);
	    commands.goToFirstPage.setEnabled(prevPage);
	    commands.goToPrevPage.setEnabled(prevPage);
	    commands.stopRendering.setEnabled(hasReport && renderInProgress);
	    commands.goToLastPage.setEnabled(nextPage);
	    commands.goToNextPage.setEnabled(nextPage);
	    commands.goToPage.setEnabled(hasPages);
	    commands.print.setEnabled(hasPages && !renderInProgress && !printInProgress);
	    commands.export.setEnabled(hasPages && !renderInProgress && !exportInProgress);
	    commands.refresh.setEnabled(hasReport);
	    commands.historyBack.setEnabled(historyManager && historyManager.canMoveBack());
	    commands.historyForward.setEnabled(historyManager && historyManager.canMoveForward());
	    commands.toggleDocumentMap.setEnabled(hasReport && documentMapState.enabled);
	    commands.toggleDocumentMap.setChecked(documentMapState.enabled && documentMapState.visible);
	    commands.toggleParametersArea.setEnabled(hasReport && parametersAreaState.enabled);
	    commands.toggleParametersArea.setChecked(parametersAreaState.enabled && parametersAreaState.visible);
	    commands.togglePrintPreview.setEnabled(hasPages).setChecked(controller.getViewMode() == distExports.ViewMode.PrintPreview);
	    commands.pageMode.setEnabled(hasPages).setChecked(controller.getPageMode() == distExports.PageMode.ContinuousScroll);
	    commands.zoom.setEnabled(hasPage);
	    commands.zoomIn.setEnabled(hasPage);
	    commands.zoomOut.setEnabled(hasPage);
	    commands.toggleZoomMode.setEnabled(hasPage).setChecked(controller.getScaleMode() == distExports.ScaleMode.FitPage || controller.getScaleMode() == distExports.ScaleMode.FitPageWidth);
	    commands.toggleSearchDialog.setEnabled(hasPages).setChecked(searchInitiated);
	    commands.toggleSendEmailDialog.setEnabled(hasPages).setChecked(sendEmailDialogState.visible);
	    commands.toggleAiPromptDialog.setEnabled(hasPages && !renderInProgress).setChecked(aiPromptInitiated);
	    notificationService.updateUI(null);
	    notificationService.pageNumberChange(currentPageNumber);
	    notificationService.pageCountChange(pageCount);
	  }
	  notificationService.setParametersAreaVisible(updateUI);
	  notificationService.setDocumentMapVisible(updateUI);
	  notificationService.updateUIInternal(updateUI);
	  notificationService.setSearchDialogVisible(updateUI);
	  notificationService.setSendEmailDialogVisible(updateUI);
	  controller.on("scaleModeChanged", (scaleMode) => {
	    commands.toggleZoomMode.setChecked(scaleMode == distExports.ScaleMode.FitPage || scaleMode === distExports.ScaleMode.FitPageWidth);
	  }).on("currentPageChanged", updateUI).onAsync("beforeLoadReport", async () => {
	    setState(stateFlags.RenderInProgress, true);
	    updateUI();
	  }).on("reportLoadProgress", updateUI).onAsync("reportLoadComplete", async () => {
	    setState(stateFlags.RenderInProgress, false);
	    updateUI();
	  }).on("reportSourceChanged", updateUI).on("viewModeChanged", updateUI).on("pageModeChanged", updateUI).on("setUIState", (args) => {
	    setState(stateFlags[args.operationName], args.inProgress);
	    updateUI();
	  }).on("error", () => {
	    setState(stateFlags.ExportInProgress, false);
	    setState(stateFlags.PrintInProgress, false);
	    setState(stateFlags.RenderInProgress, false);
	    updateUI();
	  }).on("renderingStopped", () => {
	    setState(stateFlags.RenderInProgress, false);
	    updateUI();
	  });
	  updateUI();
	}

	var defaultOptions$4 = {};
	class DocumentMapArea {
	  // #region fields    element: HTMLElement;
	  options;
	  viewerOptions;
	  controller;
	  notificationService;
	  $element;
	  $documentMap;
	  documentMapVisible;
	  enableAccessibility;
	  currentReport;
	  documentMapNecessary;
	  // #endregion
	  // #region constructor
	  constructor(element, options, viewerOptions) {
	    this.element = element;
	    this.options = $.extend({}, defaultOptions$4, options, viewerOptions);
	    this.controller = this.options.controller;
	    if (!this.controller) {
	      throw "No controller (telerikReporting.reportViewerController) has been specified.";
	    }
	    this.notificationService = this.options.notificationService;
	    if (!this.notificationService) {
	      throw "No notificationService (telerikReporting.NotificationService) has been specified.";
	    }
	    this.$element = $(this.element);
	    this.$documentMap;
	    this.documentMapVisible = this.options.documentMapVisible !== false;
	    this.enableAccessibility = this.options.enableAccessibility;
	    this.currentReport = null;
	    this.documentMapNecessary = false;
	    this.init();
	  }
	  // #endregion
	  init() {
	    this.$documentMap = $('<div id="' + this.options.viewerSelector + '-documentMap" data-role="treeview"></div>');
	    this.$documentMap.appendTo(this.element);
	    this._attachEvents();
	    this.replaceStringResources(this.$element);
	  }
	  onTreeViewSelectionChanged(event) {
	    var documentMapNode = event.sender.dataItem(event.node);
	    var page = documentMapNode.page;
	    var id = documentMapNode.id;
	    this.controller.navigateToPage(page, {
	      type: "bookmark",
	      id
	    });
	  }
	  onTreeViewNodeExpand(event) {
	    if (this.enableAccessibility) {
	      window.setTimeout(() => {
	        this.setNodeAccessibilityAttributes(event.node);
	      }, 100);
	    }
	  }
	  setNodeAccessibilityAttributes(node) {
	    var $items = $(node).find("li");
	    Array.from($items).forEach((item) => {
	      var $li = $(item);
	      $li.attr("aria-label", $li[0].innerText);
	    });
	  }
	  resetReportAndClearItems() {
	    this.documentMapNecessary = false;
	    this.showDocumentMap(false);
	    const r = this.controller.getReportSource().report;
	    const clearMapItems = this.currentReport !== r || !this.isVisible();
	    this.currentReport = r;
	    if (clearMapItems) {
	      this.clearDocumentMap();
	    }
	  }
	  clearDocumentMap() {
	    this.displayDocumentMap([]);
	  }
	  displayDocumentMap(documentMap) {
	    var hasDocumentMap = documentMap && !$.isEmptyObject(documentMap);
	    var $treeView = this.$documentMap.data("kendoTreeView");
	    if (!$treeView) {
	      try {
	        this.$documentMap.kendoTreeView({
	          dataTextField: "text",
	          select: this.onTreeViewSelectionChanged.bind(this)
	        });
	        $treeView = this.$documentMap.data("kendoTreeView");
	      } catch (e) {
	        console.error("Instantiation of Kendo TreeView as Document Map threw an exception", e);
	        throw e;
	      }
	    }
	    $treeView.setDataSource(documentMap);
	    if (this.enableAccessibility) {
	      this.setAccessibilityAttributes($treeView);
	    }
	    this.showDocumentMap(hasDocumentMap);
	  }
	  setAccessibilityAttributes(treeView) {
	    treeView.bind("expand", this.onTreeViewNodeExpand.bind(this));
	    treeView.element.attr("aria-label", stringResources.ariaLabelDocumentMap);
	    var listItems = treeView.element.find("ul");
	    Array.from(listItems).forEach((list) => {
	      this.setNodeAccessibilityAttributes(list);
	    });
	    if (this.documentMapNecessary) {
	      this.setSplitbarAccessibilityAttributes();
	    }
	  }
	  setSplitbarAccessibilityAttributes() {
	    var splitbar = this.$element.next();
	    if (this.options.documentMapAreaPosition === DocumentMapAreaPositions.RIGHT) {
	      splitbar = this.$element.prev();
	    }
	    splitbar.attr("aria-label", stringResources.ariaLabelDocumentMapSplitter);
	  }
	  isVisible() {
	    var args = {};
	    this.notificationService.getDocumentMapState(args);
	    return args.visible;
	  }
	  beginLoad() {
	    this.$element.addClass("trv-loading");
	  }
	  endLoad() {
	    this.$element.removeClass("trv-loading");
	  }
	  showDocumentMap(show) {
	    var splitter = $("#" + this.options.viewerSelector + "-document-map-splitter").getKendoSplitter();
	    var sibling = this.$element.next(".k-splitbar");
	    if (this.options.documentMapAreaPosition === DocumentMapAreaPositions.RIGHT) {
	      sibling = this.$element.prev(".k-splitbar");
	    }
	    if (splitter) {
	      (this.documentMapNecessary ? $.fn.removeClass : $.fn.addClass).call(sibling, "k-hidden");
	      splitter.toggle(".trv-document-map", show);
	    }
	  }
	  _attachEvents() {
	    this.controller.on("beforeLoadParameters", () => {
	      this.resetReportAndClearItems();
	    }).on("beginLoadReport", () => {
	      this.beginLoad();
	    }).onAsync("reportLoadComplete", async (reportInfo) => {
	      if (reportInfo.documentMapAvailable) {
	        this.documentMapNecessary = true;
	        this.displayDocumentMap(reportInfo.documentMapNodes);
	        this.notificationService.setDocumentMapVisible({ enabled: true, visible: this.documentMapVisible });
	      } else {
	        this.documentMapNecessary = false;
	        this.showDocumentMap(this.documentMapNecessary);
	      }
	      this.endLoad();
	    }).on("error", () => {
	      this.endLoad();
	      this.clearDocumentMap();
	    }).on("renderingStopped", () => {
	      this.documentMapNecessary = false;
	      this.showDocumentMap(false);
	    });
	    this.notificationService.setDocumentMapVisible((event, args) => {
	      this.documentMapVisible = args.visible;
	      this.showDocumentMap(this.documentMapVisible && this.documentMapNecessary);
	    }).getDocumentMapState((event, args) => {
	      args.enabled = this.documentMapNecessary;
	      args.visible = this.documentMapVisible;
	    });
	  }
	  replaceStringResources($documentMap) {
	    var $documentMapOverlay = $documentMap.find(".trv-document-map-overlay");
	    if (!$documentMapOverlay) {
	      return;
	    }
	    $documentMapOverlay.attr("aria-label", stringResources[$documentMapOverlay.attr("aria-label")]);
	  }
	  // #endregion
	}

	let EventEmitter$1 = class EventEmitter extends EventTarget {
	  _events;
	  _eventsCount;
	  constructor() {
	    super();
	    this._events = {};
	    this._eventsCount = 0;
	  }
	  /**
	   * @param {string} type
	   * @param {(event: CustomEvent, ...args: any[]) => void} listener
	   * @returns
	   */
	  addListener(type, listener) {
	    if (typeof listener !== "function") {
	      throw new TypeError("listener must be a function");
	    }
	    if (!this._events[type]) {
	      this._events[type] = [];
	    }
	    function wrappedListener(event) {
	      listener.call(this, event, ...event.detail);
	    }
	    wrappedListener.listener = listener;
	    this._events[type].push(wrappedListener);
	    this._eventsCount++;
	    this.addEventListener(type, wrappedListener.bind(this));
	    return this;
	  }
	  /**
	   * @alias addListener
	   * @param {string} type
	   * @param {(event: CustomEvent, ...args: any[]) => void} listener
	   * @returns
	   */
	  on(type, listener) {
	    return this.addListener(type, listener);
	  }
	  /**
	   * @param {string} type
	   * @param {any[]} args
	   * @returns
	   */
	  trigger(type, ...args) {
	    if (!this._events[type]) {
	      return void 0;
	    }
	    const event = new CustomEvent(type, {
	      detail: args,
	      cancelable: true
	    });
	    return this.dispatchEvent(event);
	  }
	  /**
	   * @alias trigger
	   * @param {string} type
	   * @param {any[]} args
	   * @returns
	   */
	  emit(type, ...args) {
	    return this.trigger(type, ...args);
	  }
	  /**
	   * @param {string} type
	   * @param {(event: CustomEvent, ...args: any[]) => void} listener
	   * @returns
	   */
	  removeListener(type, listener) {
	    if (!this._events[type]) {
	      return this;
	    }
	    this._events[type] = this._events[type].filter((wrappedListener) => {
	      if (wrappedListener.listener !== listener) {
	        return true;
	      }
	      this.removeEventListener(type, wrappedListener);
	      return false;
	    });
	    if (this._events[type].length === 0) {
	      delete this._events[type];
	      this._eventsCount--;
	    }
	    return this;
	  }
	  /**
	   * @param {string} type
	   * @returns
	   */
	  removeAllListeners(type) {
	    if (type === void 0) {
	      Object.keys(this._events).forEach((eventType) => {
	        this.removeAllListeners(eventType);
	      });
	      return this;
	    }
	    if (this._events[type]) {
	      this._events[type].forEach((wrappedListener) => {
	        this.removeEventListener(type, wrappedListener);
	      });
	      delete this._events[type];
	      this._eventsCount--;
	    }
	    return this;
	  }
	  /**
	   * @param {string} type
	   * @param {(event: CustomEvent, ...args: any[]) => void} listener
	   * @returns
	   */
	  off(type, listener) {
	    if (type === void 0) {
	      return this.removeAllListeners();
	    }
	    if (listener === void 0) {
	      return this.removeAllListeners(type);
	    }
	    return this.removeListener(type, listener);
	  }
	};

	var defaultOptions$3 = {};
	var Events = {
	  PARAMETERS_READY: "pa.parametersReady",
	  ERROR: "pa.Error"
	};
	class ParametersArea {
	  // #region fields
	  options;
	  viewerOptions;
	  element;
	  $element;
	  eventEmitter;
	  controller;
	  notificationService;
	  parameters;
	  editors;
	  parameterEditors;
	  initialParameterValues;
	  recentParameterValues;
	  processedParameterValues;
	  _parametersContainer;
	  _parametersWrapper;
	  $errorMessage;
	  $previewButton;
	  parameterContainerTemplate;
	  parametersAreaVisible;
	  parametersAreaNecessary;
	  enableAccessibility;
	  loadingCount;
	  initialized;
	  // #endregion
	  // #region constructor
	  constructor(element, options, viewerOptions) {
	    this.options = $.extend({}, defaultOptions$3, options, viewerOptions);
	    this.element = element;
	    this.eventEmitter = new EventEmitter$1();
	    this.editors = {};
	    this.controller = this.options.controller;
	    if (!this.controller) {
	      throw "No controller (telerikReporting.reportViewerController) has been specified.";
	    }
	    this.notificationService = options.notificationService;
	    if (!this.notificationService) {
	      throw "No notificationService (telerikReporting.NotificationService) has been specified.";
	    }
	    this.parameterEditors = ParameterEditors;
	    if (this.options.parameterEditors && this.options.parameterEditors.length > 0) {
	      this.parameterEditors = [].concat(this.options.parameterEditors, ParameterEditors);
	    }
	    this.recentParameterValues;
	    this.parameters;
	    this.initialParameterValues;
	    this.processedParameterValues = void 0;
	    this.$element = $(this.element);
	    this._parametersContainer = this.element?.querySelector(".trv-parameters-area-content");
	    this._parametersWrapper = this.element?.querySelector(".trv-parameters-wrapper");
	    this.$errorMessage = this.$element.find(".trv-error-message");
	    this.$previewButton = this.$element.find(".trv-parameters-area-preview-button");
	    this.$previewButton.text(stringResources[this.$previewButton.text()]);
	    this.$previewButton.attr("aria-label", stringResources[this.$previewButton.attr("aria-label")]);
	    this.$previewButton.on("click", (e) => {
	      e.preventDefault();
	      if (!this.controller.hasInvalidParameter(this.parameters)) {
	        this.applyParameters();
	      }
	    });
	    this.parameterContainerTemplate = this.options.templates["trv-parameter"];
	    this.parametersAreaVisible = this.options.parametersAreaVisible !== false;
	    this.enableAccessibility = this.options.enableAccessibility;
	    this.loadingCount = 0;
	    this.parametersAreaNecessary = false;
	    this.initialized = false;
	    this.init();
	  }
	  // #endregion
	  // #region methods
	  init() {
	    if (this.initialized) {
	      return;
	    }
	    this._attachEvents();
	    this.replaceStringResources(this.$element);
	  }
	  replaceStringResources($paramsArea) {
	    var $previewButton = $paramsArea.find(".trv-parameters-area-preview-button");
	    if (!$previewButton) {
	      return;
	    }
	    $previewButton.attr("aria-label", stringResources[$previewButton.attr("aria-label")]);
	    $previewButton.text(stringResources[$previewButton.text()]);
	  }
	  toggleElementDisplay(el, shouldHide) {
	    el.style.display = shouldHide ? "none" : "";
	  }
	  createParameterContainer() {
	    return $(this.parameterContainerTemplate);
	  }
	  createParameterUI(parameter) {
	    var $container = this.createParameterContainer();
	    var $editorPlaceholder = $container.find(".trv-parameter-value");
	    var $title = $container.find(".trv-parameter-title");
	    var $error = $container.find(".trv-parameter-error");
	    var $errorMessage = $container.find(".trv-parameter-error-message");
	    var $useDefaultValueCheckbox = $container.find(".trv-parameter-use-default input");
	    var editorsTypes = this.options.parameters && this.options.parameters.editors ? this.options.parameters.editors : null;
	    var editorFactory = this.selectParameterEditorFactory(parameter, editorsTypes);
	    var parameterText = escapeHtml(parameter.text);
	    var isHiddenParameter = !parameter.isVisible;
	    if (isHiddenParameter) {
	      parameterText += " [<b>hidden</b>]";
	    }
	    $title.html(parameterText).attr("title", parameterText);
	    $errorMessage.text(parameter.Error);
	    (parameter.Error ? $.fn.show : $.fn.hide).call($error);
	    var editor = editorFactory.createEditor(
	      $editorPlaceholder,
	      {
	        templates: this.options.templates,
	        parameterChanged: (parameter2, newValue) => {
	          var invalidParameterLI = document.querySelector(".trv-pages-area .trv-error-message ul li." + parameter2.name);
	          try {
	            newValue = ParameterValidators.validate(parameter2, newValue);
	            $error.hide();
	            if (invalidParameterLI) {
	              this.toggleElementDisplay(invalidParameterLI, true);
	            }
	            this.onParameterChanged(parameter2, newValue);
	          } catch (error) {
	            if (invalidParameterLI) {
	              this.toggleElementDisplay(invalidParameterLI, false);
	            }
	            parameter2.Error = error;
	            parameter2.value = [];
	            $errorMessage.text(error);
	            $error.show();
	            this.enablePreviewButton(false);
	          } finally {
	            this.setAccessibilityErrorState(parameter2);
	          }
	        },
	        enableAccessibility: this.enableAccessibility
	      }
	    );
	    this.editors[parameter.id] = editor;
	    editor.beginEdit(parameter);
	    if (this.enableAccessibility && !isHiddenParameter) {
	      editor.addAccessibility(parameter);
	    }
	    if ($useDefaultValueCheckbox.length > 0) {
	      $useDefaultValueCheckbox.on("click", (event) => {
	        var useDefaultValue = $(event.currentTarget).is(":checked");
	        if (useDefaultValue) {
	          delete this.recentParameterValues[parameter.id];
	          delete this.initialParameterValues[parameter.id];
	          this.invalidateChildParameters(parameter);
	          this.updateParameters(this.onLoadParametersSuccess.bind(this));
	        } else {
	          this.recentParameterValues[parameter.id] = parameter.value;
	          this.initialParameterValues[parameter.id] = parameter.value;
	        }
	        editor.enable(!useDefaultValue);
	        this.trigger(Events.PARAMETERS_READY, this.recentParameterValues);
	      });
	      var hasInitialValues = this.initialParameterValues !== null;
	      if (hasInitialValues) {
	        if (!(parameter.id in this.initialParameterValues)) {
	          $useDefaultValueCheckbox.prop("checked", true);
	          editor.enable(false);
	        }
	      } else if (isHiddenParameter) {
	        $useDefaultValueCheckbox.prop("checked", true);
	        editor.enable(false);
	      }
	    }
	    return $container;
	  }
	  setAccessibilityErrorState(parameter) {
	    var editor = this.editors[parameter.id];
	    if (!editor || !this.enableAccessibility) {
	      return;
	    }
	    editor.setAccessibilityErrorState(parameter);
	  }
	  enablePreviewButton(enabled) {
	    if (enabled) {
	      this.$previewButton.prop("disabled", false);
	      this.$previewButton.removeClass("k-disabled");
	    } else {
	      this.$previewButton.prop("disabled", true);
	      this.$previewButton.addClass("k-disabled");
	    }
	  }
	  selectParameterEditorFactory(parameter, editorsType) {
	    var factory = Array.from(this.parameterEditors).find((editor) => {
	      return editor.match(parameter, editorsType);
	    });
	    return factory;
	  }
	  showError(error) {
	    this.$errorMessage.text(error);
	    (error ? $.fn.addClass : $.fn.removeClass).call(this.$element, "trv-error");
	  }
	  showPreviewButton(parameters) {
	    (this.controller.allParametersAutoRefresh(parameters) && this.controller.autoRunEnabled ? $.fn.removeClass : $.fn.addClass).call(this.$element, "preview");
	  }
	  clearEditors() {
	    Object.entries(this.editors).forEach(([key, editor]) => {
	      if (typeof editor.destroy === "function") {
	        editor.destroy();
	      }
	    });
	    this.editors = {};
	  }
	  fill(newParameters) {
	    this.recentParameterValues = {};
	    this.processedParameterValues = {};
	    this.parameters = newParameters || [];
	    this.clearEditors();
	    var $parameterContainer;
	    var $tempContainer = $("<div></div>");
	    Array.from(this.parameters).forEach((parameter) => {
	      try {
	        parameter.value = ParameterValidators.validate(parameter, parameter.value);
	      } catch (e) {
	        parameter.Error = parameter.Error || e;
	      }
	      var hasError = Boolean(parameter.Error);
	      var hasValue = !hasError;
	      if (hasValue) {
	        this.recentParameterValues[parameter.id] = parameter.value;
	      } else {
	        parameter.Error = stringResources.invalidParameter;
	      }
	      if (parameter.availableValues) {
	        this.processedParameterValues[parameter.id] = { valueMember: parameter.value, displayMember: parameter.label, availableValues: parameter.availableValues, multivalue: parameter.multivalue, allowNull: parameter.allowNull };
	      } else {
	        this.processedParameterValues[parameter.id] = parameter.value;
	      }
	      if (parameter.isVisible || this.options.showHiddenParameters) {
	        $parameterContainer = this.createParameterUI(parameter);
	        if ($parameterContainer) {
	          $tempContainer.append($parameterContainer);
	        }
	      }
	    });
	    if (this.initialParameterValues !== void 0) {
	      if (null === this.initialParameterValues) {
	        this.initialParameterValues = {};
	        Array.from(this.parameters).forEach((parameter) => {
	          if (parameter.isVisible) {
	            this.initialParameterValues[parameter.id] = parameter.value;
	          } else {
	            delete this.recentParameterValues[parameter.id];
	          }
	        });
	      } else {
	        Array.from(this.parameters).forEach((parameter) => {
	          if (!(parameter.id in this.initialParameterValues)) {
	            delete this.recentParameterValues[parameter.id];
	          }
	        });
	      }
	    }
	    if (this._parametersWrapper) {
	      kendo.destroy(this._parametersWrapper);
	      this._parametersWrapper.innerHTML = "";
	      if (this.parameters.length > 0) {
	        this._parametersWrapper.append(...$tempContainer.children().get());
	        if (this.enableAccessibility) {
	          this._parametersWrapper.setAttribute("aria-label", "Parameters area. Contains " + this.parameters.length + " parameters.");
	        }
	      }
	    }
	    this.showPreviewButton(this.parameters);
	    var allValid = !this.controller.hasInvalidParameter(this.parameters);
	    this.enablePreviewButton(allValid);
	    this.applyProcessedParameters();
	  }
	  applyParameters() {
	    this.controller.setParameters($.extend({}, this.recentParameterValues));
	    this.controller.previewReport(false);
	  }
	  applyProcessedParameters() {
	    this.controller.setProcessedParameterValues($.extend({}, this.processedParameterValues));
	  }
	  tryRefreshReport() {
	    this.trigger(Events.PARAMETERS_READY, this.recentParameterValues);
	    if (this.controller.allParametersValidForAutoRefresh(this.parameters, this.recentParameterValues) && this.controller.autoRunEnabled) {
	      this.applyParameters();
	    }
	  }
	  invalidateChildParameters(parameter) {
	    if (parameter.childParameters) {
	      Array.from(parameter.childParameters).forEach((parameterId) => {
	        var childParameter = this.getParameterById(parameterId);
	        if (childParameter) {
	          this.invalidateChildParameters(childParameter);
	        }
	        delete this.recentParameterValues[parameterId];
	        this.resetPendingParameterChange(parameterId);
	      });
	    }
	  }
	  resetPendingParameterChange(parameterId) {
	    if (this.editors) {
	      var editor = this.editors[parameterId];
	      if (editor && typeof editor.clearPendingChange === "function") {
	        editor.clearPendingChange();
	      }
	    }
	  }
	  onParameterChanged(parameter, newValue) {
	    delete parameter["Error"];
	    parameter.value = newValue;
	    this.recentParameterValues[parameter.id] = newValue;
	    if (this.initialParameterValues !== void 0) {
	      if (parameter.id in this.initialParameterValues) {
	        this.recentParameterValues[parameter.id] = newValue;
	      }
	    } else {
	      this.recentParameterValues[parameter.id] = newValue;
	    }
	    this.invalidateChildParameters(parameter);
	    if (parameter.childParameters) {
	      this.updateParameters(this.tryRefreshReport.bind(this));
	    } else {
	      var allValid = !this.controller.hasInvalidParameter(this.parameters);
	      this.enablePreviewButton(allValid);
	      if (allValid) {
	        var errorMsg = document.querySelector(".trv-pages-area .trv-error-message");
	        if (this.$previewButton.is(":visible") && errorMsg) {
	          errorMsg.textContent = stringResources.tryReportPreview;
	        }
	        this.tryRefreshReport();
	      }
	    }
	  }
	  getParameterById(parameterId) {
	    if (this.parameters) {
	      for (var i = 0; i < this.parameters.length; i++) {
	        var p = this.parameters[i];
	        if (p.id === parameterId) {
	          return p;
	        }
	      }
	    }
	    return null;
	  }
	  setParametersAreaVisibility(visible) {
	    this.controller.setParametersAreaVisible({ visible });
	  }
	  hasVisibleParameters(params) {
	    return (params || []).some((parameter) => {
	      return parameter.isVisible;
	    });
	  }
	  beginLoad() {
	    this.loadingCount++;
	    this.$element.addClass("trv-loading");
	  }
	  endLoad() {
	    if (this.loadingCount > 0) {
	      if (0 === --this.loadingCount) {
	        this.$element.removeClass("trv-loading");
	      }
	    }
	  }
	  onLoadParametersComplete(params, successAction) {
	    this.showParametersAreaIfNecessary(params);
	    this.fill(params);
	    this.showError("");
	    if (this.parametersAreaNecessary && this.parametersAreaVisible) {
	      this.showParametersArea(true);
	      if (this.enableAccessibility) {
	        this.setSplitbarAccessibilityAttributes();
	      }
	    }
	    this.notificationService.updateUIInternal();
	    if (typeof successAction === "function") {
	      successAction();
	    }
	    this.endLoad();
	  }
	  showParametersAreaIfNecessary(params) {
	    this.parametersAreaNecessary = this.hasVisibleParameters(params) || !this.controller.autoRunEnabled;
	    this.showParametersArea(this.parametersAreaVisible && this.parametersAreaNecessary);
	  }
	  updateParameters(successAction) {
	    this.acceptParameters(this.controller.loadParameters(this.recentParameterValues), successAction);
	  }
	  acceptParameters(controllerLoadParametersPromise, successAction) {
	    this.beginLoad();
	    controllerLoadParametersPromise.then((parameters) => {
	      this.onLoadParametersComplete(parameters, successAction);
	    }).catch((error) => {
	      this.endLoad();
	      this.clear();
	      if (!this.$element.hasClass("k-hidden")) {
	        this.showError(error);
	      }
	      this.trigger(Events.ERROR, error);
	    });
	  }
	  onLoadParametersSuccess() {
	    if (this.initialParameterValues === null) {
	      this.initialParameterValues = $.extend({}, this.recentParameterValues);
	    }
	    this.trigger(Events.PARAMETERS_READY, this.recentParameterValues);
	  }
	  showParametersArea(show) {
	    var splitter = $("#" + this.options.viewerSelector + "-parameters-splitter").getKendoSplitter();
	    if (!splitter) {
	      return;
	    }
	    var sibling = this.$element.prev(".k-splitbar");
	    if (this.options.parametersAreaPosition === ParametersAreaPositions.TOP || this.options.parametersAreaPosition === ParametersAreaPositions.LEFT) {
	      sibling = this.$element.next(".k-splitbar");
	    }
	    (this.parametersAreaNecessary ? $.fn.removeClass : $.fn.addClass).call(sibling, "k-hidden");
	    splitter.toggle(".trv-parameters-area", show);
	  }
	  setSplitbarAccessibilityAttributes() {
	    var splitbar = this.$element.prev();
	    var tabIndex = this.$element.find(".trv-parameters-area-content").attr("tabIndex");
	    if (this.options.parametersAreaPosition === ParametersAreaPositions.TOP || this.options.parametersAreaPosition === ParametersAreaPositions.LEFT) {
	      splitbar = this.$element.next();
	    }
	    splitbar.attr("aria-label", stringResources.ariaLabelParametersAreaSplitter);
	    splitbar.attr("tabIndex", tabIndex);
	  }
	  _attachEvents() {
	    this.controller.on("reloadParameters", (controllerLoadParametersPromise) => {
	      this.showError();
	      if (this._parametersWrapper) {
	        kendo.destroy(this._parametersWrapper);
	        this._parametersWrapper.innerHTML = "";
	      }
	      this.acceptParameters(controllerLoadParametersPromise, this.onLoadParametersSuccess.bind(this));
	    }).onAsync("beforeLoadReport", async () => {
	      this.loadingCount = 0;
	      this.beginLoad();
	    }).onAsync("reportLoadComplete", async (reportInfo) => {
	      this.showParametersAreaIfNecessary(this.parameters);
	      this.showPreviewButton(this.parameters);
	    }).on("error", () => this.endLoad()).on("pageReady", () => this.endLoad());
	    this.notificationService.getParametersAreaState((event, args) => {
	      var parametersAreaNecessary = false;
	      if (this.parameters) {
	        parametersAreaNecessary = this.hasVisibleParameters(this.parameters) || !this.controller.autoRunEnabled;
	      }
	      args.enabled = parametersAreaNecessary;
	      args.visible = this.parametersAreaVisible;
	    }).setParametersAreaVisible((event, args) => {
	      this.parametersAreaVisible = args.visible;
	      this.showParametersArea(args.visible && (this.hasVisibleParameters(this.parameters) || !this.controller.autoRunEnabled));
	    });
	  }
	  clear() {
	    this.fill([]);
	  }
	  setParameters(parameterValues) {
	    this.initialParameterValues = null === parameterValues ? null : $.extend({}, parameterValues);
	  }
	  // #endregion
	  // #region events
	  on(type, handler) {
	    this.eventEmitter.on(type, handler);
	    return this;
	  }
	  trigger(type, ...args) {
	    return this.eventEmitter.trigger(type, ...args);
	  }
	  // #endregion
	}

	class BaseComponent {
	  // #region fields
	  element;
	  $element;
	  options;
	  // #endregion
	  // #region constructor
	  constructor(element, options = {}) {
	    this.element = element;
	    this.$element = $(element);
	    this.options = options;
	    Object.defineProperty(this.element, "__COMPONENT__", {
	      get: () => this
	    });
	  }
	  // #endregion
	}

	class Button extends BaseComponent {
	  // #region fields
	  icon;
	  click;
	  component;
	  // #endregion
	  // #region constructors
	  constructor(element, options) {
	    super(element, options);
	    this.options.icon = element.dataset.icon;
	    this.options.fillMode = element.dataset.fillMode;
	    this.component = new kendo.ui.Button(element, this.options);
	    this._initCommand();
	  }
	  // #endregion
	  // #region methods
	  _initCommand() {
	    if (!this.options.command) {
	      return;
	    }
	    const command = this.options.command;
	    this.component.setOptions({
	      click: (event) => {
	        event.preventDefault();
	        command.exec();
	      }
	    });
	    $(command).on("enabledChanged", (event, newState) => {
	      this.component.enable(newState);
	    });
	  }
	  // #endregion
	}

	class ToggleButton extends BaseComponent {
	  // #region fields
	  icon;
	  click;
	  component;
	  // #endregion
	  // #region constructors
	  constructor(element, options) {
	    super(element, options);
	    this.options.icon = element.dataset.icon;
	    this.options.fillMode = element.dataset.fillMode;
	    this.component = new kendo.ui.Button(element, this.options);
	    this._initCommand();
	  }
	  // #endregion
	  // #region methods
	  _initCommand() {
	    if (!this.options.command) {
	      return;
	    }
	    const command = this.options.command;
	    this.component.setOptions({
	      click: (event) => {
	        event.preventDefault();
	        command.setChecked(command.getChecked() ? false : true);
	      }
	    });
	    $(command).on("enabledChanged", (event, newState) => {
	      this.component.enable(newState);
	    });
	    $(command).on("checkedChanged", (event, newState) => {
	      if (newState) {
	        this.component.element.addClass("k-selected");
	      } else {
	        this.component.element.removeClass("k-selected");
	      }
	    });
	  }
	}

	var defaultOptions$2 = {};
	const componentMap = {
	  "button": Button,
	  "toggle-button": ToggleButton
	};
	function replaceStringResources$3($search) {
	  if (!$search) {
	    return;
	  }
	  var $searchOptions = $search.find(".trv-search-dialog-search-options");
	  var $searchStopButton = $search.find("button[data-command='telerik_ReportViewer_searchDialog_StopSearch']");
	  var $searchMatchCaseButton = $search.find("button[data-command='telerik_ReportViewer_searchDialog_MatchCase']");
	  var $searchMatchWholeWordButton = $search.find("button[data-command='telerik_ReportViewer_searchDialog_MatchWholeWord']");
	  var $searchUseRegexButton = $search.find("button[data-command='telerik_ReportViewer_searchDialog_UseRegex']");
	  var $searchNavigateUpButton = $search.find("button[data-command='telerik_ReportViewer_searchDialog_NavigateUp']");
	  var $searchNavigateDownButton = $search.find("button[data-command='telerik_ReportViewer_searchDialog_NavigateDown']");
	  replaceAttribute$2($search, "aria-label");
	  replaceAttribute$2($searchOptions, "aria-label");
	  replaceTitleAndAriaLabel($searchStopButton);
	  replaceTitleAndAriaLabel($searchMatchCaseButton);
	  replaceTitleAndAriaLabel($searchMatchWholeWordButton);
	  replaceTitleAndAriaLabel($searchUseRegexButton);
	  replaceTitleAndAriaLabel($searchNavigateUpButton);
	  replaceTitleAndAriaLabel($searchNavigateDownButton);
	}
	function replaceTitleAndAriaLabel($a) {
	  replaceAttribute$2($a, "title");
	  replaceAttribute$2($a, "aria-label");
	}
	function replaceAttribute$2($el, attribute) {
	  if ($el) {
	    $el.attr(attribute, stringResources[$el.attr(attribute)]);
	  }
	}
	class Search {
	  constructor(element, options, viewerOptions) {
	    this.options = $.extend({}, defaultOptions$2, options);
	    this.viewerOptions = viewerOptions;
	    this.element = element;
	    this.$element = $(element);
	    this.reportViewerWrapper = $(`[data-selector='${this.viewerOptions.viewerSelector}']`);
	    this.viewer = this.reportViewerWrapper.data("telerik_ReportViewer");
	    this.controller = this.options.controller;
	    this.notificationService = this.options.notificationService;
	    this.initialized = false;
	    this.kendoSearchDialog;
	    this.kendoComboBox;
	    this.resultsLabel;
	    this.kendoListView;
	    this.commands;
	    this.searchManager;
	    this.mruList = [];
	    this.inputComboRebinding;
	    this.searchMetadataRequested;
	    this.searchMetadataLoaded;
	    this.windowLocation;
	    this.lastSearch = "";
	    if (!this.controller) {
	      throw "No controller (telerikReporting.ReportViewerController) has been specified.";
	    }
	    if (!this.notificationService) {
	      throw "No notificationService (telerikReporting.NotificationService) has been specified.";
	    }
	    this.pagesAreaContainer = this.reportViewerWrapper.find('[data-id="trv-pages-area"]');
	    this.searchManager = new distExports.SearchManager(this.pagesAreaContainer[0], this.controller);
	    this.searchManager.on("searchComplete", this.updateResultsUI.bind(this));
	    this.init();
	  }
	  init() {
	    if (this.initialized) {
	      return;
	    }
	    replaceStringResources$3(this.$element);
	    this._initDialog();
	    this._initSearchbox();
	    this._initResultsArea();
	    this._attachCommands();
	    this._attachEvents();
	    this.updateResultsUI(null);
	    this.initialized = true;
	  }
	  _attachEvents() {
	    this.controller.on("beginLoadReport", this.closeAndClear.bind(this)).on("viewModeChanged", this.closeAndClear.bind(this));
	    this.notificationService.setSearchDialogVisible((event, args) => {
	      this.toggleSearchDialog(args.visible);
	    }).setSendEmailDialogVisible((event, args) => {
	      if (args.visible && this.controller.getSearchInitiated()) {
	        this.close();
	      }
	    });
	    $(window).on("resize", () => {
	      if (this.kendoSearchDialog && this.kendoSearchDialog.options.visible) {
	        this.storeDialogPosition();
	        this.adjustDialogPosition();
	      }
	    });
	  }
	  closeAndClear() {
	    if (this.searchMetadataRequested) {
	      return;
	    }
	    this.close();
	    this.searchMetadataLoaded = false;
	  }
	  toggleSearchDialog(show) {
	    this.controller.setSearchInitiated(show);
	    if (show) {
	      var searchMetadataOnDemand = this.viewerOptions.searchMetadataOnDemand;
	      if (searchMetadataOnDemand && !this.searchMetadataLoaded) {
	        this.searchMetadataRequested = true;
	        this.controller.onAsync("reportLoadComplete", async () => {
	          if (this.searchMetadataRequested) {
	            this.open();
	            this.searchMetadataRequested = false;
	          }
	        });
	        this.controller.refreshReport(true);
	        return;
	      }
	    }
	    this.toggle(show);
	  }
	  open() {
	    this.toggle(true);
	  }
	  close() {
	    this.toggle(false);
	  }
	  toggle(show) {
	    this.controller.setSearchInitiated(show);
	    if (show) {
	      this.searchMetadataLoaded = true;
	      this.kendoSearchDialog.open();
	    } else {
	      this.searchManager.closeSearch();
	      if (this.kendoSearchDialog && this.kendoSearchDialog.options.visible) {
	        this.kendoSearchDialog.close();
	      }
	    }
	  }
	  _initSearchbox() {
	    this.kendoComboBox = new kendo.ui.ComboBox(this.element.querySelector(".trv-search-dialog-input-box"), {
	      placeholder: stringResources.searchDialogCaptionText,
	      dataTextField: "value",
	      dataValueField: "value",
	      dataSource: this.mruList,
	      contentElement: "",
	      change: this.kendoComboBoxSelect.bind(this),
	      ignoreCase: false,
	      // the actual search-when-typing performs on this event.
	      filtering: this.onInputFiltering.bind(this),
	      filter: "startswith",
	      delay: 1e3,
	      open: (event) => {
	        if (this.inputComboRebinding) {
	          event.preventDefault();
	        }
	      },
	      select: this.processComboBoxEvent.bind(this)
	    });
	  }
	  _initDialog() {
	    this.kendoSearchDialog = new kendo.ui.Window(this.element, {
	      title: stringResources.searchDialogTitle,
	      visible: false,
	      height: 390,
	      width: 310,
	      minWidth: 310,
	      minHeight: 390,
	      maxHeight: 700,
	      scrollable: false,
	      close: (event) => {
	        this.storeDialogPosition();
	        this.kendoComboBox.value("");
	        this.updateResultsUI(null);
	        this.toggleErrorLabel(false, null);
	        this.lastSearch = "";
	      },
	      open: (event) => {
	        this.adjustDialogPosition();
	      },
	      deactivate: (event) => {
	        this.notificationService.setSearchDialogVisible({
	          visible: false
	        });
	      },
	      activate: (event) => {
	        this.kendoComboBox.input.focus();
	      }
	    });
	    this.kendoSearchDialog.element.removeClass("trv-search-dialog k-hidden");
	    this.kendoSearchDialog.wrapper.addClass("trv-search-dialog");
	  }
	  storeDialogPosition() {
	    this.windowLocation = this.kendoSearchDialog.wrapper.offset();
	  }
	  adjustDialogPosition() {
	    var windowWidth = $(window).innerWidth();
	    var windowHeight = $(window).innerHeight();
	    var width = this.kendoSearchDialog.wrapper.outerWidth(true);
	    var height = this.kendoSearchDialog.wrapper.outerHeight(true);
	    var padding = 10;
	    var scrollOverlapOffsetX = 10;
	    if (!this.windowLocation) {
	      var reportViewerCoords = this.pagesAreaContainer[0].getBoundingClientRect();
	      this.kendoSearchDialog.setOptions({
	        position: {
	          // scrollY ensures that the dialog appears at the correct place even when the viewer is rendered inside a scrollable page
	          top: reportViewerCoords.top + padding + window.scrollY,
	          left: reportViewerCoords.right - width - padding - scrollOverlapOffsetX
	        }
	      });
	    } else {
	      var left = this.windowLocation.left;
	      var top = this.windowLocation.top;
	      var right = left + width;
	      var bottom = top + height;
	      if (right > windowWidth - padding) {
	        left = Math.max(padding, windowWidth - width - padding);
	        this.kendoSearchDialog.setOptions({
	          position: {
	            left
	          }
	        });
	      }
	      if (bottom > windowHeight - padding) {
	        top = Math.max(padding, windowHeight - height - padding);
	        this.kendoSearchDialog.setOptions({
	          position: {
	            top
	          }
	        });
	      }
	    }
	  }
	  processComboBoxEvent(event) {
	    if (!(window.event || window.event.type)) {
	      return;
	    }
	    var evt = window.event;
	    if (evt.type === "keydown") {
	      event.preventDefault();
	      if (evt.keyCode === 40) {
	        this.moveListSelection(1);
	      }
	      if (evt.keyCode === 38) {
	        this.moveListSelection(-1);
	      }
	    }
	  }
	  _attachCommands() {
	    this.commands = {
	      searchDialog_MatchCase: new Command(),
	      searchDialog_MatchWholeWord: new Command(),
	      searchDialog_UseRegex: new Command(),
	      searchDialog_StopSearch: new Command(() => {
	        this.stopSearch();
	      }),
	      searchDialog_NavigateUp: new Command(() => {
	        this.moveListSelection(-1);
	      }),
	      searchDialog_NavigateDown: new Command(() => {
	        this.moveListSelection(1);
	      })
	    };
	    Object.keys(this.commands).forEach((key) => {
	      const element = this.element.querySelector(`button[data-command='telerik_ReportViewer_${key}']`);
	      const factory = componentMap[element?.dataset?.role];
	      const command = this.commands[key];
	      if (!factory) {
	        return;
	      }
	      return new factory(element, {
	        command
	      });
	    });
	    $(this.commands.searchDialog_MatchCase).on("checkedChanged", (event, newState) => {
	      this.searchForCurrentToken();
	    });
	    $(this.commands.searchDialog_MatchWholeWord).on("checkedChanged", (event, newState) => {
	      this.searchForCurrentToken();
	    });
	    $(this.commands.searchDialog_UseRegex).on("checkedChanged", (event, newState) => {
	      this.searchForCurrentToken();
	    });
	  }
	  _initResultsArea() {
	    this.resultsLabel = this.element.querySelector(".trv-search-dialog-results-label");
	    this.resultsLabel.innerText = stringResources.searchDialogNoResultsLabel;
	    this.kendoListView = new kendo.ui.ListView(this.element.querySelector(".trv-search-dialog-results-area"), {
	      selectable: true,
	      navigatable: true,
	      dataSource: [],
	      contentElement: "",
	      template: `
                <div class="k-list-item !k-align-items-start">
                    <span class="k-list-item-text">#: description #</span>
                    <span class="k-flex"></span>
                    <span class="k-flex-none">${stringResources.searchDialogPageText} #:page#</span>
                </div>
            `.trim(),
	      change: (event) => {
	        var listView = event.sender;
	        var index = listView.select().index();
	        var view = listView.dataSource.view();
	        var dataItem = view[index];
	        this.onSelectedItem(dataItem);
	        this.updateUI(index, view.length);
	      }
	    });
	    this.kendoListView.element.addClass("k-list k-list-md");
	  }
	  stopSearch() {
	    this.setStopButtonEnabledState(false);
	  }
	  setStopButtonEnabledState(enabledState) {
	    this.commands.searchDialog_StopSearch.setEnabled(enabledState);
	  }
	  onInputFiltering(event) {
	    event.preventDefault();
	    if (event.filter && event.filter.value !== this.lastSearch) {
	      this.lastSearch = event.filter.value;
	      this.searchForToken(this.lastSearch);
	    }
	  }
	  kendoComboBoxSelect(event) {
	    var newValue = event.sender.value();
	    if (newValue && this.lastSearch !== newValue) {
	      this.lastSearch = newValue;
	      this.searchForToken(this.lastSearch);
	    }
	  }
	  searchForCurrentToken() {
	    if (this.kendoComboBox) {
	      this.searchForToken(this.kendoComboBox.value());
	    }
	  }
	  searchForToken(token) {
	    this.onSearchStarted();
	    this.addToMRU(token);
	    this.searchManager.search({
	      searchToken: token,
	      matchCase: this.commands.searchDialog_MatchCase.getChecked(),
	      matchWholeWord: this.commands.searchDialog_MatchWholeWord.getChecked(),
	      useRegularExpressions: this.commands.searchDialog_UseRegex.getChecked()
	    });
	  }
	  onSearchStarted() {
	    this.resultsLabel.innerText = stringResources.searchDialogSearchInProgress;
	    this.setStopButtonEnabledState(true);
	    this.toggleErrorLabel(false, null);
	  }
	  updateResultsUI(results, error) {
	    this.setStopButtonEnabledState(false);
	    if (error) {
	      this.toggleErrorLabel(true, error);
	    }
	    this.kendoListView.dataSource.data(results || []);
	    if (results && results.length > 0) {
	      this.kendoListView.select(this.kendoListView.content.children().first());
	      this.kendoListView.trigger("change");
	    } else {
	      this.updateUI(-1, 0);
	    }
	  }
	  addToMRU(token) {
	    if (!token || token === "") {
	      return;
	    }
	    var exists = this.mruList.filter((mru) => {
	      return mru.value === token;
	    });
	    if (exists && exists.length > 0) {
	      return;
	    }
	    this.mruList.unshift({ value: token });
	    if (this.mruList.length > 10) {
	      this.mruList.pop();
	    }
	    this.inputComboRebinding = true;
	    this.kendoComboBox.dataSource.data(this.mruList);
	    this.kendoComboBox.select((item) => {
	      return item.value === token;
	    });
	    this.inputComboRebinding = false;
	  }
	  onSelectedItem(item) {
	    this.searchManager.highlightSearchItem(item);
	  }
	  updateUI(index, count) {
	    var str = count === 0 ? stringResources.searchDialogNoResultsLabel : stringFormat(stringResources.searchDialogResultsFormatLabel, [index + 1, count]);
	    this.resultsLabel.innerText = str;
	    var allowMoveUp = index > 0;
	    var allowMoveDown = index < count - 1;
	    this.commands.searchDialog_NavigateUp.setEnabled(allowMoveUp);
	    this.commands.searchDialog_NavigateDown.setEnabled(allowMoveDown);
	  }
	  moveListSelection(offset) {
	    var $selected = this.kendoListView.select();
	    if (!$selected) {
	      $selected = this.kendoListView.element.children().first();
	      this.kendoListView.select($selected);
	      this.kendoListView.trigger("change");
	    } else {
	      var index = this.kendoListView.select().trigger("change").index();
	      var view = this.kendoListView.dataSource.view();
	      var newIndex = Math.min(view.length - 1, Math.max(0, index + offset));
	      if (newIndex !== index) {
	        var dataItem = view[newIndex];
	        var element = this.kendoListView.element.find('[data-uid="' + dataItem.uid + '"]');
	        if (element) {
	          this.kendoListView.select(element);
	          this.kendoListView.trigger("change");
	          this.scrollIfNeeded(element[0], this.kendoListView.element[0]);
	        }
	      }
	    }
	  }
	  scrollIfNeeded(element, container) {
	    if (element.offsetTop - element.clientHeight < container.scrollTop) {
	      element.scrollIntoView();
	    } else {
	      var offsetBottom = element.offsetTop + element.offsetHeight;
	      var scrollBottom = container.scrollTop + container.offsetHeight;
	      if (offsetBottom > scrollBottom) {
	        container.scrollTop = offsetBottom - container.offsetHeight;
	      }
	    }
	  }
	  toggleErrorLabel(show, message) {
	    var $errorIcon = this.$element.find("i[data-role='telerik_ReportViewer_SearchDialog_Error']");
	    if (!$errorIcon || $errorIcon.length === 0) {
	      console.log(message);
	      return;
	    }
	    if (show) {
	      $errorIcon[0].title = message;
	      $errorIcon.show();
	    } else {
	      $errorIcon.hide();
	    }
	  }
	}

	var defaultOptions$1 = {};
	function replaceStringResources$2($sendEmailDialog) {
	  if (!$sendEmailDialog) {
	    return;
	  }
	  var labels = $sendEmailDialog.find(".trv-replace-string");
	  var ariaLabel = $sendEmailDialog.find("[aria-label]").add($sendEmailDialog);
	  var titles = $sendEmailDialog.find("[title]");
	  var requiredMsg = $sendEmailDialog.find("[data-required-msg]");
	  var emailMsg = $sendEmailDialog.find("[data-email-msg]");
	  if (labels.length) {
	    Array.from(labels).forEach((element) => {
	      replaceText$1($(element));
	    });
	  }
	  if (ariaLabel.length) {
	    Array.from(ariaLabel).forEach((element) => {
	      replaceAttribute$1($(element), "aria-label");
	    });
	  }
	  if (titles.length) {
	    Array.from(titles).forEach((element) => {
	      replaceAttribute$1($(element), "title");
	    });
	  }
	  if (requiredMsg.length) {
	    Array.from(requiredMsg).forEach((element) => {
	      replaceAttribute$1($(element), "data-required-msg");
	    });
	  }
	  if (emailMsg.length) {
	    Array.from(emailMsg).forEach((element) => {
	      replaceAttribute$1($(element), "data-email-msg");
	    });
	  }
	}
	function replaceText$1($el) {
	  if ($el) {
	    $el.text(stringResources[$el.text()]);
	  }
	}
	function replaceAttribute$1($el, attribute) {
	  if ($el) {
	    $el.attr(attribute, stringResources[$el.attr(attribute)]);
	  }
	}
	class SendEmail {
	  // #region fields
	  // #endregion
	  // #region constructor
	  constructor(element, options, viewerOptions) {
	    this.options = $.extend({}, defaultOptions$1, options);
	    this.viewerOptions = viewerOptions;
	    this.element = element;
	    this.$element = $(element);
	    this.viewerElement = $(`[data-selector='${this.viewerOptions.viewerSelector}']`);
	    this.viewer = this.viewerElement.data("telerik_ReportViewer");
	    this.controller = this.options.controller;
	    this.notificationService = options.notificationService;
	    this.initialized = false;
	    this.dialogVisible = false;
	    this.kendoSendEmailDialog;
	    this.inputFrom;
	    this.inputTo;
	    this.inputCC;
	    this.inputSubject;
	    this.docFormat;
	    this.docFormatEl;
	    this.bodyEditorEl;
	    this.bodyEditor;
	    this.docFormatList;
	    this.optionsCommandSet;
	    this.windowLocation;
	    if (!this.controller) {
	      throw "No controller (telerikReporting.ReportViewerController) has been specified.";
	    }
	    if (!this.notificationService) {
	      throw "No notificationService (telerikReporting.NotificationService) has been specified.";
	    }
	    if (!this.viewerOptions.sendEmail || !this.viewerOptions.sendEmail.enabled) {
	      this.viewerElement.find("[data-command='toggleSendEmailDialog']").addClass("send-email-hidden");
	      return;
	    }
	    this.init();
	  }
	  // #endregion
	  // #region methods
	  init() {
	    if (this.initialized) {
	      return;
	    }
	    replaceStringResources$2(this.$element);
	    this.controller.getDocumentFormats().then((formats) => {
	      this.docFormatList = formats;
	      this.docFormat?.setDataSource(this.docFormatList);
	      if (this.viewerOptions?.sendEmail && this.viewerOptions?.sendEmail.format) {
	        this.docFormat?.value(this.viewerOptions.sendEmail.format);
	        this.docFormat?.trigger("change");
	      }
	    });
	    this._initDialog();
	    this._initInputFields();
	    this._attachCommands();
	    this._attachEvents();
	    this.initialized = true;
	  }
	  _attachEvents() {
	    this.controller.on("beginLoadReport", this.close.bind(this)).on("viewModeChanged", this.close.bind(this));
	    this.notificationService.getSendEmailDialogState((event, args) => {
	      args.visible = this.dialogVisible;
	    }).setSendEmailDialogVisible((event, args) => {
	      if (args.visible === true) {
	        this.open();
	      } else {
	        this.close();
	      }
	    }).setSearchDialogVisible((event, args) => {
	      if (args.visible && this.dialogVisible) {
	        this.close();
	      }
	    });
	    $(window).on("resize", () => {
	      if (this.kendoSendEmailDialog && this.kendoSendEmailDialog.options.visible) {
	        this.storeDialogPosition();
	        this.adjustDialogPosition();
	      }
	    });
	  }
	  open() {
	    this.dialogVisible = true;
	    this.kendoSendEmailDialog.open();
	  }
	  close() {
	    this.dialogVisible = false;
	    if (this.kendoSendEmailDialog && this.kendoSendEmailDialog.options.visible) {
	      this.kendoSendEmailDialog.close();
	    }
	  }
	  getBody() {
	    return this.bodyEditor ? this.bodyEditor.value() : "";
	  }
	  _initDialog() {
	    this.kendoSendEmailDialog = new kendo.ui.Window(this.element, {
	      title: stringResources.sendEmailDialogTitle,
	      visible: false,
	      width: 800,
	      minWidth: 350,
	      minHeight: 350,
	      modal: true,
	      close: () => {
	        this.storeDialogPosition();
	        this.kendoValidator.reset();
	      },
	      open: () => {
	        this.adjustDialogPosition();
	      },
	      deactivate: () => {
	        this.notificationService.setSendEmailDialogVisible({
	          visible: false
	        });
	      },
	      activate: () => {
	        this.inputFrom.focus();
	        setTimeout(() => {
	          this.setValidation();
	        }, 250);
	      }
	    });
	    this.kendoSendEmailDialog.element.removeClass("trv-send-email-dialog k-hidden");
	    this.kendoSendEmailDialog.wrapper.addClass("trv-send-email-dialog");
	  }
	  _initInputFields() {
	    const prefix = this.viewerOptions.viewerSelector;
	    this.setAttrs();
	    this.inputFrom = new kendo.ui.TextBox(this.element.querySelector(`[name="${prefix}-from"]`), {});
	    this.inputTo = new kendo.ui.TextBox(this.element.querySelector(`[name="${prefix}-to"]`), {});
	    this.inputCC = new kendo.ui.TextBox(this.element.querySelector(`[name="${prefix}-cc"]`), {});
	    this.inputSubject = new kendo.ui.TextBox(this.element.querySelector(`[name="${prefix}-subject"]`), {});
	    this.docFormat = new kendo.ui.ComboBox(this.element.querySelector(`[name="${prefix}-format"]`), {
	      dataTextField: "localizedName",
	      dataValueField: "name",
	      dataSource: this.docFormatList || [],
	      filter: "startswith",
	      dataBound: (event) => {
	        event.sender.select(0);
	        event.sender.trigger("change");
	      }
	    });
	    this.bodyEditor = new kendo.ui.Editor(this.element.querySelector(`[name="${prefix}-emailBody"]`), {
	      tools: [
	        "bold",
	        "italic",
	        "underline",
	        "strikethrough",
	        "justifyLeft",
	        "justifyCenter",
	        "justifyRight",
	        "justifyFull",
	        "insertUnorderedList",
	        "insertOrderedList",
	        "indent",
	        "outdent",
	        "createLink",
	        "unlink",
	        "cleanFormatting",
	        "formatting",
	        "fontName",
	        "fontSize",
	        "foreColor",
	        "backColor",
	        "subscript",
	        "superscript"
	      ]
	    });
	    this.setDefaultValues(this.viewerOptions.sendEmail);
	    this.kendoValidator = new kendo.ui.Validator(this.element.querySelector(".trv-send-email-fields"), {});
	  }
	  setAttrs() {
	    const prefix = this.viewerOptions.viewerSelector;
	    Array.from(this.element.querySelectorAll(".trv-send-email-field input, .trv-send-email-field textarea")).forEach((element) => {
	      var attrName = element.getAttribute("name");
	      if (attrName === null) {
	        return;
	      }
	      element.setAttribute("id", `${prefix}-${attrName}`);
	      element.setAttribute("name", `${prefix}-${attrName}`);
	    });
	    Array.from(this.element.querySelectorAll(".trv-send-email-label label")).forEach((element) => {
	      var attrFor = element.getAttribute("for");
	      if (attrFor === null) {
	        return;
	      }
	      element.setAttribute("for", `${prefix}-${attrFor}`);
	    });
	  }
	  storeDialogPosition() {
	    var kendoWindow = this.kendoSendEmailDialog.element.parent(".k-window");
	    this.windowLocation = kendoWindow.offset();
	  }
	  adjustDialogPosition() {
	    var windowWidth = $(window).innerWidth();
	    var windowHeight = $(window).innerHeight();
	    var width = this.kendoSendEmailDialog.wrapper.outerWidth(true);
	    var height = this.kendoSendEmailDialog.wrapper.outerHeight(true);
	    var padding = 10;
	    if (!this.windowLocation) {
	      this.kendoSendEmailDialog.center();
	    } else {
	      var left = this.windowLocation.left;
	      var top = this.windowLocation.top;
	      var right = left + width;
	      var bottom = top + height;
	      if (right > windowWidth - padding) {
	        left = Math.max(padding, windowWidth - width - padding);
	        this.kendoSendEmailDialog.setOptions({
	          position: {
	            left
	          }
	        });
	      }
	      if (bottom > windowHeight - padding) {
	        top = Math.max(padding, windowHeight - height - padding);
	        this.kendoSendEmailDialog.setOptions({
	          position: {
	            top
	          }
	        });
	      }
	    }
	  }
	  _attachCommands() {
	    this.optionsCommandSet = {
	      "sendEmail_Cancel": new Command(() => {
	        this.close();
	      }),
	      "sendEmail_Send": new Command(() => {
	        this.sendingEmail();
	      })
	    };
	    Binder.attachCommands(this.$element.find(".trv-send-email-actions"), this.optionsCommandSet, this.viewerOptions);
	  }
	  sendingEmail(cmd, args) {
	    var sendEmailArgs = {
	      from: this.inputFrom.value(),
	      to: this.inputTo.value(),
	      cc: this.inputCC.value(),
	      subject: this.inputSubject.value(),
	      format: this.docFormat.value(),
	      body: this.getBody(),
	      deviceInfo: {}
	    };
	    if (this.validateFields()) {
	      this.controller.sendReport(sendEmailArgs);
	      this.close();
	    }
	  }
	  setValidation() {
	    this.inputFrom.element.off("blur").on("blur", () => {
	      if (!this.isEmpty(this.inputFrom)) {
	        this.isValidEmail(this.inputFrom, false);
	      }
	    });
	    this.inputTo.element.off("blur").on("blur", () => {
	      if (!this.isEmpty(this.inputTo)) {
	        this.isValidEmail(this.inputTo, true);
	      }
	    });
	    this.inputCC.element.off("blur").on("blur", () => {
	      if (this.inputCC.value().toString().length) {
	        this.isValidEmail(this.inputCC, true);
	      } else {
	        this.hideError(this.inputCC.element);
	      }
	    });
	  }
	  validateFields() {
	    var fromIsValid = !this.isEmpty(this.inputFrom) && this.isValidEmail(this.inputFrom, false);
	    var toIsValid = !this.isEmpty(this.inputTo) && this.isValidEmail(this.inputTo, true);
	    var ccIsValid = this.isEmpty(this.inputCC) || this.isValidEmail(this.inputCC, true);
	    var hasFormat = this.docFormat.value().length > 0;
	    if (!hasFormat) {
	      this.showError(this.docFormat.element, "data-required-msg");
	    }
	    return fromIsValid && toIsValid && ccIsValid && hasFormat;
	  }
	  /* Sets all default email values except the format as it depends on a request */
	  setDefaultValues(sendEmail) {
	    this.inputFrom.value(sendEmail && sendEmail.from || "");
	    this.inputTo.value(sendEmail && sendEmail.to || "");
	    this.inputCC.value(sendEmail && sendEmail.cc || "");
	    this.inputSubject.value(sendEmail && sendEmail.subject || "");
	    this.bodyEditor.value(sendEmail && sendEmail.body || "");
	  }
	  isEmpty(input) {
	    var $el = input.element;
	    if (!input.value().length) {
	      this.showError($el, "data-required-msg");
	      return true;
	    }
	    this.hideError($el);
	    return false;
	  }
	  showError($el, tag) {
	    var validationMsg = stringResources[$el.attr(tag)];
	    $('[data-for="' + $el.attr("name") + '"]').addClass("-visible").text(validationMsg);
	  }
	  hideError($el) {
	    $('[data-for="' + $el.attr("name") + '"]').removeClass("-visible");
	  }
	  isValidEmail(input, moreThenOneEmail) {
	    var inputValue = input.value();
	    var $el = input.element;
	    if (moreThenOneEmail) {
	      var listEmailsAddress = inputValue.split(/[\s,;]+/);
	      for (var i = 0; i < listEmailsAddress.length; i++) {
	        if (!this._validateEmail(listEmailsAddress[i].trim(), $el)) {
	          return false;
	        }
	      }
	      return true;
	    }
	    return this._validateEmail(inputValue, $el);
	  }
	  _validateEmail(email, $el) {
	    var regexEmail = /\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*/;
	    if (email.indexOf(",") > -1 || email.indexOf(";") > -1) {
	      this.showError($el, "data-single-email-msg");
	      return false;
	    }
	    if (!regexEmail.test(email)) {
	      this.showError($el, "data-email-msg");
	      return false;
	    }
	    return true;
	  }
	}

	function getEventHandlerFromArguments(args) {
	  var arg0;
	  if (args && args.length) {
	    arg0 = args[0];
	  }
	  if (typeof arg0 === "function") {
	    return arg0;
	  }
	  return null;
	}
	class EventEmitter {
	  _events;
	  constructor() {
	    this._events = {};
	  }
	  resolveEvent(eventName) {
	    var event = this._events[eventName];
	    if (!event) {
	      this._events[eventName] = event = new Event(eventName);
	    }
	    return event;
	  }
	  on(eventName, handler) {
	    this.resolveEvent(eventName).on(handler);
	  }
	  trigger(eventName, args) {
	    this.resolveEvent(eventName).trigger(args);
	  }
	}
	class Event {
	  _callbacks;
	  _eventName;
	  constructor(eventName) {
	    this._callbacks = [];
	    this._eventName = eventName;
	  }
	  on(callback) {
	    this._callbacks.push(callback);
	  }
	  trigger(args) {
	    const eventName = this._eventName;
	    this._callbacks.forEach(function(callback) {
	      callback.apply(null, [eventName, ...args]);
	    });
	  }
	}
	class NotificationService {
	  _eventEmitter;
	  constructor() {
	    this._eventEmitter = new EventEmitter();
	  }
	  eventFactory(event, args) {
	    var handler = getEventHandlerFromArguments(args);
	    if (handler) {
	      this._eventEmitter.on(event, handler);
	    } else {
	      this._eventEmitter.trigger(event, args);
	    }
	    return this;
	  }
	  Events = {
	    GET_DOCUMENT_MAP_STATE: "trv.GET_DOCUMENT_MAP_STATE",
	    SET_DOCUMENT_MAP_VISIBLE: "trv.SET_DOCUMENT_MAP_VISIBLE",
	    GET_PARAMETER_AREA_STATE: "trv.GET_PARAMETER_AREA_STATE",
	    SET_PARAMETER_AREA_VISIBLE: "trv.SET_PARAMETER_AREA_VISIBLE",
	    SET_TOGGLE_SIDE_MENU: "trv.SET_TOGGLE_SIDE_MENU",
	    GET_TOGGLE_SIDE_MENU: "trv.GET_TOGGLE_SIDE_MENU",
	    UPDATE_UI: "trv.UPDATE_UI",
	    CSS_LOADED: "trv.CSS_LOADED",
	    UPDATE_UI_INTERNAL: "trv.UPDATE_UI_INTERNAL",
	    PAGE_NUMBER: "trv.PAGE_NUMBER",
	    PAGE_COUNT: "trv.PAGE_COUNT",
	    GET_SEARCH_DIALOG_STATE: "trv.GET_SEARCH_DIALOG_STATE",
	    GET_SEND_EMAIL_DIALOG_STATE: "trv.GET_SEND_EMAIL_DIALOG_STATE",
	    SET_SEARCH_DIALOG_VISIBLE: "trv.SET_SEARCH_DIALOG_VISIBLE",
	    SET_SEND_EMAIL_DIALOG_VISIBLE: "trv.SET_SEND_EMAIL_DIALOG_VISIBLE",
	    SET_AI_PROMPT_DIALOG_VISIBLE: "trv.SET_AI_PROMPT_DIALOG_VISIBLE"
	  };
	  on() {
	    const eventEmitter = this._eventEmitter;
	    eventEmitter.on.apply(eventEmitter, arguments);
	  }
	  getDocumentMapState() {
	    return this.eventFactory(this.Events.GET_DOCUMENT_MAP_STATE, arguments);
	  }
	  setDocumentMapVisible() {
	    return this.eventFactory(this.Events.SET_DOCUMENT_MAP_VISIBLE, arguments);
	  }
	  getParametersAreaState() {
	    return this.eventFactory(this.Events.GET_PARAMETER_AREA_STATE, arguments);
	  }
	  setParametersAreaVisible() {
	    return this.eventFactory(this.Events.SET_PARAMETER_AREA_VISIBLE, arguments);
	  }
	  cssLoaded() {
	    return this.eventFactory(this.Events.CSS_LOADED, arguments);
	  }
	  updateUI() {
	    return this.eventFactory(this.Events.UPDATE_UI, arguments);
	  }
	  updateUIInternal() {
	    return this.eventFactory(this.Events.UPDATE_UI_INTERNAL, arguments);
	  }
	  pageNumberChange() {
	    return this.eventFactory(this.Events.PAGE_NUMBER, arguments);
	  }
	  pageCountChange() {
	    return this.eventFactory(this.Events.PAGE_COUNT, arguments);
	  }
	  getSendEmailDialogState() {
	    return this.eventFactory(this.Events.GET_SEND_EMAIL_DIALOG_STATE, arguments);
	  }
	  setSearchDialogVisible() {
	    return this.eventFactory(this.Events.SET_SEARCH_DIALOG_VISIBLE, arguments);
	  }
	  setSendEmailDialogVisible() {
	    return this.eventFactory(this.Events.SET_SEND_EMAIL_DIALOG_VISIBLE, arguments);
	  }
	  setAiPromptDialogVisible() {
	    return this.eventFactory(this.Events.SET_AI_PROMPT_DIALOG_VISIBLE, arguments);
	  }
	}

	class MemStorage {
	  // #region fields
	  _data = {};
	  // #endregion
	  // #region properties
	  get length() {
	    return Object.keys(this._data).length;
	  }
	  // #endregion
	  // #region constructor
	  constructor() {
	    this._data = {};
	  }
	  // #endregion
	  // #region methods
	  getItem(key) {
	    return this._data[key] || null;
	  }
	  setItem(key, value) {
	    this._data[key] = value;
	  }
	  removeItem(key) {
	    delete this._data[key];
	  }
	  key(index) {
	    return Object.keys(this._data)[index] || null;
	  }
	  clear() {
	    this._data = {};
	  }
	  // #endregion
	}

	const TemplateCache = function() {
	  var cache = {};
	  return {
	    load: (url, serviceUrl, client) => {
	      var p = cache[url];
	      if (!p) {
	        cache[url] = p = client.get(url).then((html) => {
	          var templates = {};
	          var styleSheets = [];
	          var baseUri = rTrim(serviceUrl, "\\/") + "/";
	          html = replaceAll(html, "{service}/", baseUri);
	          html = replaceAll(html, "{service}", baseUri);
	          var viewerTemplate = $("<div></div>").html(html);
	          Array.from(viewerTemplate.find("template")).forEach((element) => {
	            var $element = $(element);
	            templates[$element.attr("id")] = trim($element.html(), "\n 	");
	          });
	          Array.from(viewerTemplate.find("link")).forEach((element) => {
	            styleSheets.push(trim(element.outerHTML, "\n 	"));
	          });
	          styleSheets = filterUniqueLastOccurrence(styleSheets);
	          return {
	            templates,
	            styleSheets
	          };
	        });
	      }
	      return p;
	    }
	  };
	}();

	class ReportViewerSettings {
	  // #region fields
	  _id;
	  _storage;
	  _defaults;
	  // #endregion
	  // #region constructor
	  constructor(id, storage, defaultSettings) {
	    this._id = id;
	    this._storage = storage;
	    this._defaults = defaultSettings || {};
	  }
	  // #endregion
	  // #region methods
	  formatKey(key) {
	    return this._id + "_" + key;
	  }
	  getItem(key) {
	    var value = this._storage.getItem(this.formatKey(key));
	    if (typeof value === "string") {
	      try {
	        value = JSON.parse(value);
	      } catch (e) {
	        value = null;
	      }
	    }
	    return value !== null && value !== void 0 ? value : this._defaults[key];
	  }
	  setItem(key, value) {
	    var formattedKey = this.formatKey(key);
	    this._storage.setItem(formattedKey, JSON.stringify(value));
	  }
	  // #endregion
	  // #region accessors
	  getViewMode() {
	    return this.getItem("viewMode");
	  }
	  setViewMode(value) {
	    this.setItem("viewMode", value);
	  }
	  getPageMode() {
	    return this.getItem("pageMode");
	  }
	  setPageMode(value) {
	    this.setItem("pageMode", value);
	  }
	  getPrintMode() {
	    return this.getItem("printMode");
	  }
	  setPrintMode(value) {
	    this.setItem("printMode", value);
	  }
	  getScale() {
	    return this.getItem("scale");
	  }
	  setScale(value) {
	    this.setItem("scale", value);
	  }
	  getScaleMode() {
	    return this.getItem("scaleMode");
	  }
	  setScaleMode(value) {
	    this.setItem("scaleMode", value);
	  }
	  getDocumentMapVisible() {
	    return this.getItem("documentMapVisible");
	  }
	  setDocumentMapVisible(value) {
	    this.setItem("documentMapVisible", value);
	  }
	  getParametersAreaVisible() {
	    return this.getItem("parametersAreaVisible");
	  }
	  setParametersAreaVisible(value) {
	    this.setItem("parametersAreaVisible", value);
	  }
	  getHistory() {
	    return this.getItem("history");
	  }
	  setHistory(value) {
	    this.setItem("history", value);
	  }
	  getClientId() {
	    return this.getItem("clientId");
	  }
	  setClientId(value) {
	    this.setItem("clientId", value);
	  }
	  getReportSource() {
	    return this.getItem("reportSource");
	  }
	  setReportSource(value) {
	    this.setItem("reportSource", value);
	  }
	  getPageNumber() {
	    return this.getItem("pageNumber");
	  }
	  setPageNumber(value) {
	    this.setItem("pageNumber", value);
	  }
	  getEnableAccessibility() {
	    return this.getItem("enableAccessibility");
	  }
	  setEnableAccessibility(value) {
	    this.setItem("enableAccessibility", value);
	  }
	  getAccessibilityKeyMap() {
	    return this.getItem("accessibilityKeyMap");
	  }
	  setAccessibilityKeyMap(value) {
	    this.setItem("accessibilityKeyMap", value);
	  }
	  getSearchMetadataOnDemand() {
	    return this.getItem("searchMetadataOnDemand");
	  }
	  setSearchMetadataOnDemand(value) {
	    this.setItem("searchMetadataOnDemand", value);
	  }
	  getKeepClientAlive() {
	    return this.getItem("keepClientAlive");
	  }
	  setKeepClientAlive(value) {
	    this.setItem("keepClientAlive", value);
	  }
	  // #endregion
	}

	class LinkButton extends BaseComponent {
	  // #region fields
	  cmd;
	  // #endregion
	  // #region constructor
	  constructor(element, options) {
	    super(element, options);
	    var dataCommand = this.$element.attr("data-command");
	    if (dataCommand) {
	      this.cmd = this.options.commands[dataCommand];
	    }
	    if (this.cmd) {
	      this.$element.on("click", (event) => {
	        if (this.cmd.getEnabled()) {
	          this.cmd.exec($(event.currentTarget).attr("data-command-parameter"));
	        } else {
	          event.preventDefault();
	        }
	      });
	      $(this.cmd).on("enabledChanged", (event) => {
	        (this.cmd.getEnabled() ? $.fn.removeClass : $.fn.addClass).call(this.$element, "disabled");
	      }).on("checkedChanged", (event) => {
	        (this.cmd.getChecked() ? $.fn.addClass : $.fn.removeClass).call(this.$element, "checked");
	      });
	    }
	  }
	  // #endregion
	}

	class PageNumberInput extends BaseComponent {
	  // #region fields
	  cmd;
	  _numeric;
	  // #endregion
	  // #region constructor
	  constructor(element, options) {
	    super(element, options);
	    this.cmd = this.options.commands["goToPage"];
	    this._numeric = new kendo.ui.NumericTextBox(this.element, {
	      format: "0",
	      decimals: 0,
	      min: 0,
	      spinners: false,
	      change: this._onChange.bind(this),
	      spin: this._onChange.bind(this)
	    });
	    this._numeric._text[0].dataset.role = "telerik_ReportViewer_PageNumberInput";
	    this._numeric.element[0].dataset.role = "";
	    this._numeric.wrapper[0].classList.add("trv-toolbar-numeric");
	    this._numeric._text[0].setAttribute("aria-label", stringResources.ariaLabelPageNumberEditor);
	    this.options.controller.onAsync("reportLoadComplete", async (reportInfo) => {
	      this._numeric.max(reportInfo.pageCount);
	      this._numeric.min(Math.min(1, reportInfo.pageCount));
	      this._numeric.value(Math.min(1, reportInfo.pageCount));
	    }).on("loadedReportChange", () => {
	      this._numeric.min(0);
	      this._numeric.max(0);
	      this._numeric.value(0);
	    }).on("renderingStopped", () => {
	      this._numeric.min(0);
	      this._numeric.max(0);
	      this._numeric.value(0);
	    });
	    this.options.notificationService.pageNumberChange((event, value) => {
	      var pageNumber = value;
	      var pageCount = this.options.controller.getPageCount();
	      this.element.parentElement.setAttribute("aria-label", stringFormat(stringResources.ariaLabelPageNumberSelector, [pageNumber, pageCount]));
	      this._numeric.value(value);
	    });
	  }
	  // #endregion
	  // #region event handlers
	  _onChange(event, data) {
	    var val = this._numeric.value();
	    var num = distExports.tryParseInt(val);
	    if (!isNaN(num)) {
	      this.cmd.exec(num);
	    }
	  }
	  _onSpin(event, data) {
	    return this._onChange(event, data);
	  }
	  // #endregion
	}

	class PageCountLabel extends BaseComponent {
	  // #region constructor
	  constructor(element, options) {
	    super(element, options);
	    options.notificationService.pageCountChange((event, value) => {
	      this.$element.text(value);
	    });
	  }
	  // #endregion
	}

	function TouchBehavior(dom, options) {
	  var startDistance;
	  var ignoreTouch;
	  init(dom);
	  function init(element) {
	    if (typeof $.fn.kendoTouch === "function") {
	      try {
	        $(element).on("mousedown", () => {
	          ignoreTouch = true;
	        }).on("mouseup", () => {
	          ignoreTouch = false;
	        }).kendoTouch({
	          multiTouch: true,
	          enableSwipe: true,
	          swipe: (e) => {
	            if (!ignoreTouch) {
	              onSwipe(e);
	            }
	          },
	          gesturestart: (e) => {
	            if (!ignoreTouch) {
	              onGestureStart(e);
	            }
	          },
	          gestureend: (e) => {
	            if (!ignoreTouch) {
	              onGestureEnd(e);
	            }
	          },
	          gesturechange: (e) => {
	            if (!ignoreTouch) {
	              onGestureChange(e);
	            }
	          },
	          doubletap: (e) => {
	            if (!ignoreTouch) {
	              onDoubleTap(e);
	            }
	          },
	          touchstart: (e) => {
	            if (!ignoreTouch) {
	              fire("touchstart");
	            }
	          }
	        });
	      } catch (e) {
	        console.error("Instantiation of Kendo Touch threw an exception", e);
	        throw e;
	      }
	    }
	  }
	  function onDoubleTap(e) {
	    fire("doubletap", e);
	  }
	  function onGestureStart(e) {
	    startDistance = kendo.touchDelta(e.touches[0], e.touches[1]).distance;
	  }
	  function onGestureEnd(e) {
	  }
	  function onGestureChange(e) {
	    var current = kendo.touchDelta(e.touches[0], e.touches[1]).distance;
	    onPinch({
	      distance: current,
	      lastDistance: startDistance
	    });
	    startDistance = current;
	  }
	  function onSwipe(e) {
	    fire("swipe", e);
	  }
	  function onPinch(e) {
	    fire("pinch", e);
	  }
	  function fire(func, args) {
	    var handler = options[func];
	    if (typeof handler === "function") {
	      handler(args);
	    }
	  }
	}

	const ToolBarConstants = {
	  TitleAttr: "title",
	  AriaLabelAttr: "aria-label",
	  DataCommandAttr: "data-command",
	  DataCommandParameterAttr: "data-command-parameter",
	  ExportDropdownId: "export-dropdown",
	  PageNumberInputDataRoleSelector: '[data-role="telerik_ReportViewer_PageNumberInput"]',
	  PageCountLabelDataRoleSelector: '[data-role="telerik_ReportViewer_PageCountLabel"]',
	  Button: "button",
	  ButtonGroup: "buttonGroup",
	  ToggleButton: "toggle-button",
	  DropDownButton: "dropDownButton",
	  MenuButton: "menu-button",
	  Separator: "separator",
	  CustomElement: "custom-element"
	};

	function replaceStringResources$1($toolbar) {
	  if (!$toolbar) {
	    return;
	  }
	  const labels = $toolbar.find(".trv-replace-string");
	  if (labels && labels.length > 0) {
	    Array.from(labels).forEach((element) => {
	      const $element = $(element);
	      const stringResource = stringResources[$element.text()];
	      if ($element && stringResource) {
	        $element.text(stringResource);
	      }
	    });
	  }
	}
	class Toolbar {
	  // #region fields
	  _options;
	  /** @type {HTMLElement} */
	  _element;
	  $element;
	  _kendoToolbar;
	  _neverOverflowToolBarButtons = ["goToPrevPage", "goToNextPage"];
	  // #endregion
	  // #region constructor
	  constructor(element, options, viewerOptions) {
	    this._element = element;
	    this._options = options;
	    this.$element = $(this._element);
	    if (this._kendoToolbar === void 0) {
	      this.initKendoToolbar();
	    }
	    this._options.controller.on("beginLoadReport", () => {
	      const kendoExportDropDown = this.$element.find(`#${ToolBarConstants.ExportDropdownId}`).data("kendoDropDownButton");
	      kendoExportDropDown?.enable(false);
	    }).onAsync("reportLoadComplete", async () => {
	      if (viewerOptions.renderingExtensions === void 0) {
	        this._options.controller.getDocumentFormats().then((extensions) => {
	          this.updateExportDropdownItems(extensions);
	        }).catch((error) => {
	          this._options.controller.trigger("error", { message: error });
	        });
	      } else {
	        this.updateExportDropdownItems(viewerOptions.renderingExtensions);
	      }
	    });
	  }
	  // #endregion
	  // #region methods
	  initKendoToolbar() {
	    replaceStringResources$1(this.$element);
	    const children = Array.from(this._element.children);
	    const toolBarItems = [];
	    children.forEach((child) => toolBarItems.push(this.createToolbarItem(child)));
	    this.$element.empty();
	    this._kendoToolbar = this.$element.kendoToolBar({
	      items: toolBarItems,
	      click: (e) => {
	        this.executeCommand(e);
	      },
	      toggle: (e) => {
	        this.executeCommand(e);
	      }
	    }).data("kendoToolBar");
	    const pageNumberInputEl = this.$element.find(ToolBarConstants.PageNumberInputDataRoleSelector).get(0);
	    if (pageNumberInputEl) {
	      new PageNumberInput(pageNumberInputEl, this._options);
	    }
	    const pageCountLabelEl = this.$element.find(ToolBarConstants.PageCountLabelDataRoleSelector).get(0);
	    if (pageCountLabelEl) {
	      new PageCountLabel(pageCountLabelEl, this._options);
	    }
	  }
	  createToolbarItem(elementData) {
	    const cmdName = (elementData.dataset?.command || "").replace("telerik_ReportViewer_", "");
	    if (cmdName === "toggleAiPromptDialog" && !this._options.controller.isAiInsightsEnabled()) {
	      return;
	    }
	    const id = elementData.id;
	    const role = elementData.dataset?.role;
	    const icon = elementData.dataset?.icon;
	    const overflow = elementData.dataset?.overflow;
	    const title = stringResources[elementData.title] || elementData.title || "";
	    const ariaLabel = stringResources[elementData.ariaLabel] || elementData.ariaLabel || "";
	    const cmdParam = elementData.dataset?.commandParameter;
	    const command = this._options.commands[cmdName];
	    switch (role) {
	      case ToolBarConstants.Separator:
	        return {
	          type: ToolBarConstants.Separator,
	          overflow: overflow || "auto"
	        };
	      case ToolBarConstants.CustomElement:
	        elementData.title = title;
	        elementData.ariaLabel = ariaLabel;
	        return {
	          template: elementData.outerHTML
	        };
	      case ToolBarConstants.MenuButton:
	        return {
	          id,
	          type: ToolBarConstants.DropDownButton,
	          icon,
	          enable: false,
	          showArrowButton: true,
	          overflow: "never",
	          fillMode: "flat",
	          menuButtons: [
	            { text: "Loading...", enabled: false }
	          ]
	        };
	      case ToolBarConstants.ButtonGroup:
	        const groupChildren = Array.from(elementData.children);
	        const buttonGroupButtons = [];
	        groupChildren.forEach((groupChild) => {
	          if (groupChild.dataset?.role !== ToolBarConstants.Button && groupChild.dataset?.role !== ToolBarConstants.ToggleButton) {
	            console.warn("The data-role attribute of element nested inside a buttonGroup is missing or not supported. Only 'button' or 'toggle-button' data-role is allowed.");
	            return;
	          }
	          buttonGroupButtons.push(this.createToolbarItem(groupChild));
	        });
	        return {
	          type: ToolBarConstants.ButtonGroup,
	          buttons: buttonGroupButtons,
	          fillMode: "flat",
	          overflow: overflow ? overflow : "auto"
	        };
	      case ToolBarConstants.Button:
	      case ToolBarConstants.ToggleButton:
	        $(command).on("enabledChanged", (e, newState) => {
	          this._kendoToolbar.enable($(`button[${ToolBarConstants.DataCommandAttr}="${cmdName}"]`), newState);
	        }).on("checkedChanged", (e, newState) => {
	          this._kendoToolbar.toggle($(`button[${ToolBarConstants.DataCommandAttr}="${cmdName}"]`), newState);
	        });
	        return {
	          id,
	          type: ToolBarConstants.Button,
	          text: title,
	          icon,
	          attributes: {
	            [ToolBarConstants.DataCommandAttr]: cmdName,
	            [ToolBarConstants.DataCommandParameterAttr]: cmdParam,
	            [ToolBarConstants.AriaLabelAttr]: ariaLabel,
	            [ToolBarConstants.TitleAttr]: title
	          },
	          fillMode: "flat",
	          togglable: role === ToolBarConstants.ToggleButton,
	          overflow: overflow ? overflow : "auto",
	          showText: "overflow"
	        };
	      default:
	        console.error(`The data-role attribute is missing or not supported: ${role}`);
	        return {};
	    }
	  }
	  executeCommand(event) {
	    const cmdName = event.target.get(0)?.dataset.command || "";
	    const cmdParam = event.target.get(0)?.dataset.commandParameter || "";
	    if (!cmdName) {
	      return;
	    }
	    const cmdFn = this._options.commands[cmdName];
	    if (!cmdFn) {
	      console.error(`The provided data-command attribute is not present within the commands collection: ${cmdName}`);
	      return;
	    }
	    cmdFn.exec(cmdParam);
	  }
	  updateExportDropdownItems(extensions) {
	    const kendoExportDropDown = this.$element.find(`#${ToolBarConstants.ExportDropdownId}`).data("kendoDropDownButton");
	    if (!kendoExportDropDown) {
	      return;
	    }
	    if (!(extensions && extensions.length > 0)) {
	      console.warn("No extensions found - the export dropdown button will remain disabled.");
	      return;
	    }
	    const exportDropdownItems = [];
	    extensions.forEach((extension) => exportDropdownItems.push({
	      id: extension.name,
	      text: extension.localizedName,
	      attributes: {
	        [ToolBarConstants.DataCommandAttr]: "export",
	        [ToolBarConstants.DataCommandParameterAttr]: extension.name
	      }
	    }));
	    const options = kendoExportDropDown.options;
	    options.items = exportDropdownItems;
	    kendoExportDropDown.setOptions(options);
	    kendoExportDropDown.enable(true);
	  }
	}

	var defaultOptions = {};
	function replaceStringResources($aiDialogsWrapper) {
	  if (!$aiDialogsWrapper) {
	    return;
	  }
	  var labels = $aiDialogsWrapper.find(".trv-replace-string");
	  var ariaLabel = $aiDialogsWrapper.find("[aria-label]").add($aiDialogsWrapper);
	  var titles = $aiDialogsWrapper.find("[title]");
	  if (labels.length) {
	    Array.from(labels).forEach((element) => {
	      replaceText($(element));
	    });
	  }
	  if (ariaLabel.length) {
	    Array.from(ariaLabel).forEach((element) => {
	      replaceAttribute($(element), "aria-label");
	    });
	  }
	  if (titles.length) {
	    Array.from(titles).forEach((element) => {
	      replaceAttribute($(element), "title");
	    });
	  }
	}
	function replaceText($el) {
	  if ($el) {
	    $el.text(stringResources[$el.text()]);
	  }
	}
	function replaceAttribute($el, attribute) {
	  if ($el) {
	    $el.attr(attribute, stringResources[$el.attr(attribute)]);
	  }
	}
	class AiPrompt {
	  constructor(element, options, viewerOptions) {
	    this.options = $.extend({}, defaultOptions, options);
	    this.viewerOptions = viewerOptions;
	    this.controller = this.options.controller;
	    if (!this.controller) {
	      throw "No controller (telerikReporting.ReportViewerController) has been specified.";
	    }
	    this.notificationService = this.options.notificationService;
	    if (!this.notificationService) {
	      throw "No notificationService (telerikReporting.NotificationService) has been specified.";
	    }
	    this.element = element;
	    this.$element = $(element);
	    this.kendoAiConsentDialog;
	    this.kendoAiPromptDialog;
	    this.kendoAiPrompt;
	    this.kendoAiPromtDialogLocation;
	    this.reportViewerWrapper = $(`[data-selector='${this.viewerOptions.viewerSelector}']`);
	    this.pagesAreaContainer = this.reportViewerWrapper.find('[data-id="trv-pages-area"]');
	    this.aiPromptDialogInitialized = false;
	    this.aiPromptInitialized = false;
	    this.requireConsent = false;
	    this.allowCustomPrompts = true;
	    this.predefinedPrompts = [];
	    this.init();
	  }
	  init() {
	    if (this.aiPromptDialogInitialized) {
	      return;
	    }
	    replaceStringResources(this.$element);
	    this._initAiConsentDialog();
	    this._attachAiConsentDialogCommands();
	    this._initAiPromptDialog();
	    this._attachEvents();
	    this.aiPromptDialogInitialized = true;
	  }
	  _attachEvents() {
	    this.controller.on("beginLoadReport", this.close.bind(this)).on("viewModeChanged", this.close.bind(this));
	    this.notificationService.setAiPromptDialogVisible((event, args) => {
	      this.toggleAiPromptDialog(args.visible);
	    }).setSendEmailDialogVisible((event, args) => {
	      if (args.visible && this.controller.getAiPromptInitiated()) {
	        this.close();
	      }
	    });
	    $(window).on("resize", () => {
	      if (this.kendoAiPromptDialog && this.kendoAiPromptDialog.options.visible) {
	        this.storeDialogPosition();
	        this.adjustDialogPosition(this.kendoAiPromptDialog);
	      }
	    });
	  }
	  _attachAiConsentDialogCommands() {
	    const optionsCommandSet = {
	      "aiConsent_Cancel": new Command(() => {
	        this.kendoAiConsentDialog.close();
	        this.notificationService.setAiPromptDialogVisible({
	          visible: false
	        });
	      }),
	      "aiConsent_Accept": new Command(() => {
	        this.kendoAiConsentDialog.close();
	        this.controller.saveToSessionStorage("trvAiConsent", "true");
	        this.controller.setAiPromptInitiated(true);
	        if (this.kendoAiPromptDialog) {
	          this._initAiPrompt(this.predefinedPrompts);
	          this.kendoAiPromptDialog.open();
	        }
	      })
	    };
	    Binder.attachCommands(this.kendoAiConsentDialog.element.find(".trv-ai-consent-actions"), optionsCommandSet, this.viewerOptions);
	  }
	  _initAiConsentDialog() {
	    const aiConsentDialogElement = this.element.querySelector(".trv-ai-consent-dialog");
	    if (!aiConsentDialogElement) {
	      console.warn('Failed to initialize AI consent dialog due to missing element with class "trv-ai-consent-dialog".');
	      return;
	    }
	    this.kendoAiConsentDialog = new kendo.ui.Window(aiConsentDialogElement, {
	      title: stringResources["aiPromptDialogConsentTitle"] || "",
	      width: 500,
	      minWidth: 400,
	      minHeight: 106,
	      maxHeight: 800,
	      resizable: false,
	      scrollable: true,
	      open: (event) => {
	        this.adjustDialogPosition(this.kendoAiConsentDialog);
	      }
	    });
	    this.kendoAiConsentDialog.element.removeClass("trv-ai-consent-dialog k-hidden");
	    this.kendoAiConsentDialog.wrapper.addClass("trv-ai-consent-dialog");
	  }
	  _initAiPromptDialog() {
	    const aiPromptDialogElement = this.element.querySelector(".trv-ai-prompt-dialog");
	    if (!aiPromptDialogElement) {
	      console.warn('Failed to initialize AI prompt dialog due to missing element with class "trv-ai-prompt-dialog".');
	      return;
	    }
	    this.kendoAiPromptDialog = new kendo.ui.Window(aiPromptDialogElement, {
	      title: false,
	      visible: false,
	      draggable: {
	        dragHandle: ".k-prompt-header"
	      },
	      height: "fit-content",
	      width: 500,
	      minWidth: 400,
	      maxHeight: 800,
	      scrollable: true,
	      close: (event) => {
	        this.storeDialogPosition();
	      },
	      open: (event) => {
	        this.adjustDialogPosition(this.kendoAiPromptDialog);
	      },
	      deactivate: (event) => {
	        this.notificationService.setAiPromptDialogVisible({
	          visible: false
	        });
	      }
	    });
	    this.kendoAiPromptDialog.element.removeClass("trv-ai-prompt-dialog k-hidden");
	    this.kendoAiPromptDialog.wrapper.addClass("trv-ai-prompt-dialog");
	  }
	  _initAiPrompt(promptSuggestions) {
	    if (this.aiPromptInitialized) {
	      this.kendoAiPrompt.setOptions({
	        promptSuggestions
	      });
	      this.kendoAiPrompt.activeView(this.kendoAiPrompt.activeView());
	    } else {
	      const aiPromptElement = this.kendoAiPromptDialog?.element?.find("#trv-ai-prompt");
	      if (!aiPromptElement) {
	        console.warn('Failed to initialize AI prompt dialog due to missing element with id "trv-ai-prompt".');
	        return;
	      }
	      const that = this;
	      this.kendoAiPrompt = new kendo.ui.AIPrompt(aiPromptElement, {
	        activeView: 0,
	        promptRequest: function(event) {
	          if (!event.prompt) {
	            this.activeView(0);
	            return;
	          }
	          that.controller.getAIResponse(event.prompt).then((response) => {
	            this.addPromptOutput(that.createPromptOutputFromResponse(response, event));
	            this.activeView(1);
	          }).catch((error) => {
	            this.addPromptOutput(that.createPromptOutputFromResponse(error?._responseJSON?.exceptionMessage || error?._responseJSON?.ExceptionMessage, event));
	            this.activeView(1);
	          });
	        },
	        toolbarItems: [
	          { type: "spacer" },
	          { type: "button", icon: "x", fillMode: "flat", themeColor: "primary", click: (e) => {
	            this.close();
	          } }
	        ],
	        promptSuggestions,
	        views: [
	          {
	            type: "prompt",
	            messages: {
	              promptPlaceholder: "Enter your prompt"
	            }
	          },
	          {
	            type: "output"
	          }
	        ]
	      });
	      this.aiPromptInitialized = true;
	    }
	    this.disableAiPromptTextArea(promptSuggestions && promptSuggestions.length > 0);
	    const aiPromptToolbar = this.kendoAiPrompt.element.find(".k-prompt-header .k-toolbar").data("kendoToolBar");
	    aiPromptToolbar && aiPromptToolbar.bind("toggle", (e) => {
	      if (e.target.text() === "Ask AI") {
	        this.disableAiPromptTextArea(promptSuggestions && promptSuggestions.length > 0);
	      }
	    });
	  }
	  disableAiPromptTextArea(hasPromptSuggestions) {
	    if (this.allowCustomPrompts) {
	      return;
	    }
	    let aiPromptTextAreaPlaceholder = stringResources["aiPromptDialogTextAreaPlaceholder"];
	    const aiPromptTextArea = this.kendoAiPrompt.element.find(".k-prompt-content .k-prompt-view textarea");
	    if (!hasPromptSuggestions) {
	      const aiPromptGenerateButton = this.kendoAiPrompt.element.find(".k-prompt-footer .k-actions");
	      aiPromptGenerateButton && aiPromptGenerateButton.addClass("k-disabled");
	      aiPromptTextAreaPlaceholder = stringResources["aiPromptDialogNoPredefinedAndCustomPromptsPlaceholder"] || "";
	    } else {
	      aiPromptTextAreaPlaceholder = stringResources["aiPromptDialogNoCustomPromptsPlaceholder"] || "";
	    }
	    aiPromptTextArea && aiPromptTextArea.attr("placeholder", aiPromptTextAreaPlaceholder) && aiPromptTextArea.addClass("k-disabled");
	  }
	  storeDialogPosition() {
	    this.kendoAiPromtDialogLocation = this.kendoAiPromptDialog.wrapper.offset();
	  }
	  adjustDialogPosition(dialog) {
	    var windowWidth = $(window).innerWidth();
	    var windowHeight = $(window).innerHeight();
	    var width = dialog.wrapper.outerWidth(true);
	    var height = dialog.wrapper.outerHeight(true);
	    var padding = 10;
	    var scrollOverlapOffsetX = 10;
	    if (!this.kendoAiPromtDialogLocation) {
	      var reportViewerCoords = this.pagesAreaContainer[0].getBoundingClientRect();
	      dialog.setOptions({
	        position: {
	          // scrollY ensures that the dialog appears at the correct place even when the viewer is rendered inside a scrollable page
	          top: reportViewerCoords.top + padding + window.scrollY,
	          left: reportViewerCoords.right - width - padding - scrollOverlapOffsetX
	        }
	      });
	    } else {
	      var left = this.kendoAiPromtDialogLocation.left;
	      var top = this.kendoAiPromtDialogLocation.top;
	      var right = left + width;
	      var bottom = top + height;
	      if (right > windowWidth - padding) {
	        left = Math.max(padding, windowWidth - width - padding);
	        dialog.setOptions({
	          position: {
	            left
	          }
	        });
	      }
	      if (bottom > windowHeight - padding) {
	        top = Math.max(padding, windowHeight - height - padding);
	        this.kendoAiPromptDialog.setOptions({
	          position: {
	            top
	          }
	        });
	      }
	    }
	  }
	  toggleAiPromptDialog(show) {
	    if (show) {
	      this.open();
	    } else {
	      this.close();
	    }
	  }
	  open() {
	    this.controller.createAIThread().then((data) => {
	      this.predefinedPrompts = data?.predefinedPrompts;
	      this.allowCustomPrompts = data?.allowCustomPrompts;
	      if (this.kendoAiConsentDialog && data.requireConsent && this.controller.loadFromSessionStorage("trvAiConsent") !== "true") {
	        $(".trv-ai-consent-content").html(data?.consentMessage);
	        this.kendoAiConsentDialog.open();
	        return;
	      }
	      if (this.kendoAiPromptDialog) {
	        this.controller.setAiPromptInitiated(true);
	        this._initAiPrompt(this.predefinedPrompts);
	        this.kendoAiPromptDialog.open();
	      }
	    });
	  }
	  close() {
	    this.controller.setAiPromptInitiated(false);
	    if (this.kendoAiConsentDialog) {
	      this.kendoAiConsentDialog.close();
	    }
	    if (this.kendoAiPromptDialog && this.kendoAiPromptDialog.options.visible) {
	      this.kendoAiPromptDialog.close();
	    }
	  }
	  createPromptOutputFromResponse(response, promptData) {
	    return {
	      id: kendo.guid(),
	      output: response,
	      prompt: promptData.prompt,
	      isRetry: promptData.isRetry
	    };
	  }
	}

	var viewModeReverseMap = {
	  [distExports.ViewMode.Interactive]: ViewModes.INTERACTIVE,
	  [distExports.ViewMode.PrintPreview]: ViewModes.PRINT_PREVIEW
	};
	var viewModeMap = {
	  [ViewModes.INTERACTIVE]: distExports.ViewMode.Interactive,
	  [ViewModes.PRINT_PREVIEW]: distExports.ViewMode.PrintPreview
	};
	var pageModeReverseMap = {
	  [distExports.PageMode.SinglePage]: PageModes.SINGLE_PAGE,
	  [distExports.PageMode.ContinuousScroll]: PageModes.CONTINUOUS_SCROLL
	};
	var pageModeMap = {
	  [PageModes.SINGLE_PAGE]: distExports.PageMode.SinglePage,
	  [PageModes.CONTINUOUS_SCROLL]: distExports.PageMode.ContinuousScroll
	};
	var scaleModeReverseMap = {
	  [distExports.ScaleMode.FitPageWidth]: ScaleModes.FIT_PAGE_WIDTH,
	  [distExports.ScaleMode.FitPage]: ScaleModes.FIT_PAGE,
	  [distExports.ScaleMode.Specific]: ScaleModes.SPECIFIC
	};
	var scaleModeMap = {
	  [ScaleModes.FIT_PAGE_WIDTH]: distExports.ScaleMode.FitPageWidth,
	  [ScaleModes.FIT_PAGE]: distExports.ScaleMode.FitPage,
	  [ScaleModes.SPECIFIC]: distExports.ScaleMode.Specific
	};
	var printModeReverseMap = {
	  [distExports.PrintMode.AutoSelect]: PrintModes.AUTO_SELECT,
	  [distExports.PrintMode.ForcePDFPlugin]: PrintModes.FORCE_PDF_PLUGIN,
	  [distExports.PrintMode.ForcePDFFile]: PrintModes.FORCE_PDF_FILE
	};
	var printModeMap = {
	  [PrintModes.AUTO_SELECT]: distExports.PrintMode.AutoSelect,
	  [PrintModes.FORCE_PDF_PLUGIN]: distExports.PrintMode.ForcePDFPlugin,
	  [PrintModes.FORCE_PDF_FILE]: distExports.PrintMode.ForcePDFFile
	};
	var commonMessages = parseMessages();
	var TOOLIP_SHOW_TIMEOUT = 100;
	function parseMessages() {
	  var result = {};
	  Object.keys(stringResources).forEach((key) => {
	    let firstLetter = key.charAt(0).toUpperCase();
	    result[`ReportViewer_${firstLetter}${key.slice(1)}`] = stringResources[key];
	  });
	  return result;
	}
	function getDefaultOptions(serviceUrl, version) {
	  return {
	    id: null,
	    serviceUrl: null,
	    templateUrl: rTrim(serviceUrl, "\\/") + "/resources/templates/telerikReportViewerTemplate-" + version + ".html/",
	    reportSource: null,
	    reportServer: null,
	    authenticationToken: null,
	    sendEmail: null,
	    scale: 1,
	    scaleMode: ScaleModes.SPECIFIC,
	    viewMode: ViewModes.INTERACTIVE,
	    pageMode: PageModes.CONTINUOUS_SCROLL,
	    parametersAreaPosition: ParametersAreaPositions.RIGHT,
	    documentMapAreaPosition: DocumentMapAreaPositions.LEFT,
	    parameters: {
	      editors: {
	        multiSelect: ParameterEditorTypes.LIST_VIEW,
	        singleSelect: ParameterEditorTypes.LIST_VIEW
	      }
	    },
	    persistSession: false,
	    parameterEditors: [],
	    disabledButtonClass: null,
	    checkedButtonClass: null,
	    parametersAreaVisible: true,
	    documentMapVisible: true,
	    enableAccessibility: false,
	    searchMetadataOnDemand: false,
	    initialPageAreaImageUrl: null,
	    keepClientAlive: true,
	    webDesignerPreview: false,
	    serverPreview: false
	  };
	}
	function ReportViewer(dom, options) {
	  var svcApiUrl = options.serviceUrl;
	  var reportServerUrlSVCApiUrl = "";
	  if (options.reportServer) {
	    reportServerUrlSVCApiUrl = rTrim(options.reportServer.url, "\\/");
	    svcApiUrl = reportServerUrlSVCApiUrl + "/api/reports";
	  }
	  var $placeholder = $(dom);
	  var templates = {};
	  var persistanceKey = options.id || "#" + $placeholder.attr("id");
	  var accessibility;
	  var settings = {};
	  var notificationService = {};
	  var client = {};
	  var controller = {};
	  var perspectiveManager;
	  var history = {};
	  var commands = {};
	  var viewer = {};
	  options.viewerSelector = "reportViewer-" + generateGuidString();
	  $placeholder.attr("data-selector", options.viewerSelector);
	  if (!validateOptions(options)) {
	    return;
	  }
	  var version = "20.0.26.304";
	  options = $.extend({}, getDefaultOptions(svcApiUrl, version), options);
	  settings = new ReportViewerSettings(
	    persistanceKey,
	    options.persistSession ? window.sessionStorage : new MemStorage(),
	    {
	      scale: options.scale,
	      scaleMode: options.scaleMode,
	      printMode: options.printMode ? options.printMode : options.directPrint,
	      enableAccessibility: options.enableAccessibility,
	      searchMetadataOnDemand: options.searchMetadataOnDemand,
	      sendEmail: options.sendEmail,
	      parametersAreaPosition: options.parametersAreaPosition,
	      documentMapAreaPosition: options.documentMapAreaPosition,
	      keepClientAlive: options.keepClientAlive,
	      webDesignerPreview: options.webDesignerPreview,
	      serverPreview: options.serverPreview
	    }
	  );
	  notificationService = new NotificationService();
	  client = new distExports.ServiceClient(options);
	  controller = new distExports.ReportController(
	    client,
	    new distExports.ReportControllerOptions(
	      options.keepClientAlive,
	      options.authenticationToken,
	      options.reportSource,
	      printModeMap[options.printMode],
	      pageModeMap[options.pageMode],
	      viewModeMap[options.viewMode],
	      scaleModeMap[options.scaleMode],
	      options.scale,
	      commonMessages,
	      options.enableAccessibility,
	      options.searchMetadataOnDemand
	    )
	  );
	  history = new HistoryManager({
	    controller,
	    settings
	  });
	  commands = new CommandSet({
	    controller,
	    notificationService,
	    history
	  });
	  new UIController({
	    controller,
	    notificationService,
	    history,
	    commands
	  });
	  viewer = {
	    /**
	     * String resources used by the viewer for localization and UI text.
	     * Contains key-value pairs for all UI strings, supporting localization and customization.
	     * @type {Object.<string, string>}
	     */
	    stringResources,
	    /**
	     * Refreshes the currently loaded report.
	     * Forces the viewer to reload the report from the server. Optionally ignores the cache.
	     * @function
	     * @param {boolean} [ignoreCache=true] - If true, the report is reloaded from the server, bypassing any cached data.
	     * @returns {ReportViewerInstance} Returns the viewer instance for chaining.
	     * @example // Refresh the report, ignoring cache (default)
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.refreshReport();
	     * @example // Refresh the report, using cache if available
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.refreshReport(false);
	     */
	    refreshReport: (ignoreCache) => {
	      if (arguments.length === 0) {
	        ignoreCache = true;
	      }
	      controller.refreshReport(ignoreCache, "", true);
	      return viewer;
	    },
	    /**
	     * Gets or sets the report source for the viewer.
	     * When called with an argument, sets the report source
	     * When called without arguments, gets the current report source - report and parameters. The viewer's report source is updated dynamically as the users navigate between reports and change parameter values.
	     * @function
	     * @param {ReportSource|null} [rs] - The report source object to set, or null to clear. If omitted, acts as a getter.
	     * @returns {ReportSource} Returns the viewer instance for chaining when setting, or the current report source when getting.
	     * @example // Get the current report source
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var currentSource = viewer.reportSource();
	     * @example // Set a new report source
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.reportSource({
	     *   report: "MyReport",
	     *   parameters: { StartDate: "2024-01-01", EndDate: "2024-12-31" }
	     * });
	     * @example // Clear the report source
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.reportSource(null);
	     */
	    reportSource: (rs) => {
	      if (rs || rs === null) {
	        controller.setReportSource(rs);
	        controller.refreshReport(false, "", true);
	        return viewer;
	      }
	      return controller.getReportSource();
	    },
	    /**
	     * Clears the current reportSource from the viewer's internal state and its persisted session in the browser.
	     * Called in order to force the viewer to respect the newly set reportSource on a server postback.
	     * @function
	     * @returns {ReportViewerInstance} Returns the viewer instance for chaining.
	     * @example // Clear the report source and force the viewer to respect a new reportSource on postback
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.clearReportSource();
	     */
	    clearReportSource: () => {
	      controller.setReportSource(new distExports.ReportSourceOptions());
	      return viewer;
	    },
	    /**
	     * Gets or sets the view mode of the viewer (e.g., Interactive or Print Preview).
	     * @function
	     * @param {ViewModes} [vm] - The view mode to set. If omitted, returns the current view mode.
	     * @returns {string} Returns the viewer instance for chaining when setting, or the current view mode string when getting.
	     * @example // Get the current view mode
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var mode = viewer.viewMode();
	     * @example // Set the view mode to Print Preview
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.viewMode(telerikReportViewer.ViewModes.PRINT_PREVIEW);
	     */
	    viewMode: (vm) => {
	      if (vm) {
	        controller.setViewMode(viewModeMap[vm]);
	        return viewer;
	      }
	      return viewModeReverseMap[controller.getViewMode()];
	    },
	    /**
	     * Gets or sets the page mode (e.g., Single Page or Continuous Scroll).
	     * @function
	     * @param {PageModes} [psm] - The page mode to set. If omitted, returns the current page mode.
	     * @returns {string} Returns the viewer instance for chaining when setting, or the current page mode string when getting.
	     * @example // Get the current page mode
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var pageMode = viewer.pageMode();
	     * @example // Set the page mode to Single Page
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.pageMode(telerikReportViewer.PageModes.SINGLE_PAGE);
	     */
	    pageMode: (psm) => {
	      if (psm) {
	        controller.setPageMode(pageModeMap[psm]);
	        return viewer;
	      }
	      return pageModeReverseMap[controller.getPageMode()];
	    },
	    /**
	     * Gets or sets the print mode for the viewer.
	     * @function
	     * @param {PrintModes} [pm] - The print mode to set. If omitted, returns the current print mode.
	     * @returns {string} Returns the viewer instance for chaining when setting, or the current print mode string when getting.
	     * @example // Get the current print mode
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var printMode = viewer.printMode();
	     * @example // Set the print mode to Force PDF File
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.printMode(telerikReportViewer.PrintModes.FORCE_PDF_FILE);
	     */
	    printMode: (pm) => {
	      if (pm) {
	        controller.setPrintMode(printModeMap[pm]);
	        return viewer;
	      }
	      return printModeReverseMap[controller.getPrintMode()];
	    },
	    /**
	     * Gets or sets the scale and scale mode for the viewer.
	     * @function
	     * @param {Object} [args] - The scale settings to apply. Should contain 'scale' (number) and 'scaleMode' (string).
	     * @param {number} [args.scale] - The zoom scale (e.g., 1.0 for 100%).
	     * @param {ScaleModes} [args.scaleMode] - The scale mode (e.g., 'FIT_PAGE', 'FIT_PAGE_WIDTH', 'SPECIFIC').
	     * @returns {Object} Returns the viewer instance for chaining when setting, or an object with current scale and scaleMode when getting.
	     * @example // Get the current scale and scale mode
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var scaleInfo = viewer.scale();
	     * @example // Set scale to 1.2 and scale mode to FIT_PAGE_WIDTH
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.scale({ scale: 1.2, scaleMode: telerikReportViewer.ScaleModes.FIT_PAGE_WIDTH });
	     */
	    scale: (args) => {
	      if (args) {
	        controller.setScale(args.scale);
	        controller.setScaleMode(scaleModeMap[args.scaleMode]);
	        return viewer;
	      }
	      return {
	        scale: controller.getScale(),
	        scaleMode: scaleModeReverseMap[controller.getScaleMode()]
	      };
	    },
	    /**
	     * Gets the viewer’s current page that is displayed.
	     * @function
	     * @returns {number} The current page number (1-based).
	     * @example // Get the current page number
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var page = viewer.currentPage();
	     */
	    currentPage: () => {
	      return controller.getCurrentPageNumber();
	    },
	    /**
	     * Gets the total page count of the viewer’s currently displayed report.
	     * @function
	     * @returns {number} The total page count.
	     * @example // Get the total number of pages in the current report
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var totalPages = viewer.pageCount();
	     */
	    pageCount: () => {
	      return controller.getPageCount();
	    },
	    /**
	     * Shows or hides the parameters area in the viewer UI.
	     * @function
	     * @param {boolean} visible - True to show, false to hide the parameters area.
	     * @returns {void}
	     * @example // Show the parameters area
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.parametersAreaVisible(true);
	     * @example // Hide the parameters area
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.parametersAreaVisible(false);
	     */
	    parametersAreaVisible: (visible) => {
	      notificationService.setParametersAreaVisible({ visible });
	    },
	    /**
	     * Returns an object with properties, the names of which are the report parameters’ IDs, and values - the report parameters’ values. The values of multivalue parameters include their display and value members.
	     * @function
	     * @returns {Array<Object>} An array of report parameter objects.
	     * @example // Get all report parameters and log them
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var parameters = viewer.getReportParameters();
	     * console.log(parameters);
	     */
	    getReportParameters: () => {
	      return controller.getReportParameters();
	    },
	    /**
	     * Returns whether auto-run is enabled for the report (i.e., whether the report runs automatically when parameters change).
	     * @function
	     * @returns {boolean} True if auto-run is enabled, false otherwise.
	     * @example // Check if auto-run is enabled for the report
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var isReportAutoRun = viewer.isReportAutoRun();
	     */
	    isReportAutoRun: () => {
	      return controller.autoRunEnabled;
	    },
	    /**
	     * Sets the authentication token. Used for service requests.
	     * @function
	     * @param {string} [token] - The authentication token to set.
	     * @returns {ReportViewerInstance} Returns the viewer instance for chaining.
	     * @example // Set the authentication token for the viewer
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.authenticationToken("YOUR_AUTH_TOKEN");
	     */
	    authenticationToken: (token) => {
	      if (token) {
	        controller.setAuthenticationToken(token);
	      }
	      return viewer;
	    },
	    /**
	     * Binds event handlers to the specified event.
	     * @function
	     * @param {string} eventName - The name of the event to bind to (see Events constants).
	     * @param {function} eventHandler - The handler function to invoke when the event occurs.
	     * @returns {void}
	     * @example // Bind to the 'pageReady' event
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.bind(telerikReportViewer.Events.PAGE_READY, function(e, args) {
	     *   console.log("Page is ready!", args);
	     * });
	     * @example // Bind to the 'error' event
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.bind(telerikReportViewer.Events.ERROR, function(e, args) {
	     *   alert("An error occurred: " + args);
	     * });
	     */
	    bind: (eventName, eventHandler) => {
	      eventBinder(eventName, eventHandler, true);
	    },
	    /**
	     * Unbinds an event handler from a viewer event.
	     * @function
	     * @param {string} eventName - The name of the event to unbind from.
	     * @param {function} [eventHandler] - The handler function to remove. If omitted, all handlers for the event are removed.
	     * @returns {void}
	     * @example // Unbind all handlers from the 'error' event
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.unbind(telerikReportViewer.Events.ERROR);
	     */
	    unbind: (eventName, eventHandler) => {
	      eventBinder(eventName, eventHandler, false);
	    },
	    /**
	     * Gets or sets the accessibility key map for keyboard navigation.
	     * Allows customization of keyboard shortcuts for accessibility features.
	     * @function
	     * @param {Object} [keyMap] - The key map object to set. If omitted, acts as a getter.
	     * @returns {Object} Returns the viewer instance for chaining when setting, the current key map when getting, or undefined if accessibility is not enabled.
	     * @example // Get the current accessibility key map
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * var keyMap = viewer.accessibilityKeyMap();
	     *
	     * @example // Set a custom accessibility key map
	     * var reportViewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * reportViewer.accessibilityKeyMap(
	     * {
	     *     CONFIRM_KEY: 32,
	     *     CONTENT_AREA_KEY: 82, //R
	     *     DOCUMENT_MAP_AREA_KEY: 77, //M
	     *     MENU_AREA_KEY: 85, //U
	     *     PARAMETERS_AREA_KEY: 80 //P
	     * });
	     */
	    accessibilityKeyMap: (keyMap) => {
	      if (accessibility) {
	        if (keyMap) {
	          accessibility.setKeyMap(keyMap);
	          return viewer;
	        }
	        return accessibility.getKeyMap();
	      }
	      return void 0;
	    },
	    /**
	     * The command set for the viewer, providing programmatic access to built-in commands (e.g., navigation, export).
	     * @type {ReportViewerCommands}
	     */
	    commands,
	    /**
	     * Disposes the viewer instance, cleaning up resources and event handlers.
	     * After calling this method, the viewer instance should not be used.
	     * Stops sending keep alive requests to the server, if keep client alive was enabled.
	     * @function
	     * @returns {void}
	     * @example
	     *  // Dispose the viewer instance when it is no longer needed
	     * var viewer = $("#reportViewer1").data("telerik_ReportViewer");
	     * viewer.dispose();
	     */
	    dispose: () => {
	      clearTooltipTimeout();
	      controller.disposeSentinel();
	      controller.destroy();
	      if (perspectiveManager && typeof perspectiveManager.dispose === "function") {
	        perspectiveManager.dispose();
	      }
	    }
	  };
	  function validateOptions(options2) {
	    if (!options2) {
	      $placeholder.text("The report viewer configuration options are not initialized.");
	      return false;
	    }
	    if (options2.reportServer) {
	      if (!options2.reportServer.url) {
	        $placeholder.text("The report server URL is not specified.");
	        return false;
	      }
	    } else {
	      if (!options2.serviceUrl) {
	        $placeholder.text("The serviceUrl is not specified.");
	        return false;
	      }
	    }
	    return true;
	  }
	  function eventBinder(eventName, eventHandler, bind) {
	    if (typeof eventHandler === "function") {
	      if (bind) {
	        $(viewer).on(eventName, { sender: viewer }, eventHandler);
	      } else {
	        $(viewer).off(eventName, eventHandler);
	      }
	    } else if (!eventHandler && !bind) {
	      $(viewer).off(eventName);
	    }
	  }
	  function attachEvents() {
	    const viewerEventsMapping = {
	      [Events$1.EXPORT_BEGIN]: "exportStarted",
	      [Events$1.EXPORT_END]: "exportDocumentReady",
	      [Events$1.PRINT_BEGIN]: "printStarted",
	      [Events$1.PRINT_END]: "printDocumentReady",
	      [Events$1.SEND_EMAIL_BEGIN]: "sendEmailStarted",
	      [Events$1.SEND_EMAIL_END]: "sendEmailDocumentReady",
	      [Events$1.PAGE_READY]: "pageReady",
	      [Events$1.ERROR]: "error",
	      [Events$1.INTERACTIVE_ACTION_EXECUTING]: "interactiveActionExecuting",
	      [Events$1.INTERACTIVE_ACTION_ENTER]: "interactiveActionEnter",
	      [Events$1.INTERACTIVE_ACTION_LEAVE]: "interactiveActionLeave",
	      [Events$1.PARAMETERS_LOADED]: "parametersLoaded"
	      // UPDATE_UI, // Raised below by notificationService
	      // VIEWER_TOOLTIP_OPENING // Raised in showTooltip() method
	    };
	    var viewerAsyncEventsMapping = {
	      [Events$1.RENDERING_BEGIN]: "beforeLoadReport",
	      [Events$1.RENDERING_END]: "reportLoadComplete"
	    };
	    var $viewer = $(viewer);
	    const eventProxy = (eventName) => async (args) => {
	      $viewer.trigger(
	        {
	          type: eventName,
	          data: { sender: viewer }
	        },
	        args
	      );
	    };
	    for (let eventName in viewerEventsMapping) {
	      let controllerEventName = viewerEventsMapping[eventName];
	      controller.on(controllerEventName, (args) => {
	        if (eventName === Events$1.PAGE_READY) {
	          args.pageActions = JSON.stringify(args.pageActions);
	        }
	        eventProxy(eventName)(args);
	      });
	    }
	    for (let eventName in viewerAsyncEventsMapping) {
	      let controllerEventName = viewerAsyncEventsMapping[eventName];
	      controller.onAsync(controllerEventName, eventProxy(eventName));
	    }
	    notificationService.on(notificationService.Events.UPDATE_UI, function($viewer2) {
	      return function(e, args) {
	        $viewer2.trigger(
	          {
	            type: Events$1.UPDATE_UI,
	            data: e.data
	          },
	          args
	        );
	      };
	    }($viewer));
	  }
	  function attachEventHandlers() {
	    eventBinder(Events$1.EXPORT_BEGIN, options.exportBegin, true);
	    eventBinder(Events$1.EXPORT_END, options.exportEnd, true);
	    eventBinder(Events$1.PRINT_BEGIN, options.printBegin, true);
	    eventBinder(Events$1.PRINT_END, options.printEnd, true);
	    eventBinder(Events$1.SEND_EMAIL_BEGIN, options.sendEmailBegin, true);
	    eventBinder(Events$1.SEND_EMAIL_END, options.sendEmailEnd, true);
	    eventBinder(Events$1.RENDERING_BEGIN, options.renderingBegin, true);
	    eventBinder(Events$1.RENDERING_END, options.renderingEnd, true);
	    eventBinder(Events$1.PAGE_READY, options.pageReady, true);
	    eventBinder(Events$1.ERROR, options.error, true);
	    eventBinder(Events$1.UPDATE_UI, options.updateUi, true);
	    eventBinder(Events$1.INTERACTIVE_ACTION_EXECUTING, options.interactiveActionExecuting, true);
	    eventBinder(Events$1.INTERACTIVE_ACTION_ENTER, options.interactiveActionEnter, true);
	    eventBinder(Events$1.INTERACTIVE_ACTION_LEAVE, options.interactiveActionLeave, true);
	    eventBinder(Events$1.VIEWER_TOOLTIP_OPENING, options.viewerToolTipOpening, true);
	    eventBinder(Events$1.PARAMETERS_LOADED, options.parametersLoaded, true);
	  }
	  function init() {
	    $placeholder.html(templates["trv-report-viewer"]);
	    Binder.bind(
	      $placeholder,
	      {
	        controller,
	        notificationService,
	        commands,
	        templates
	      },
	      options
	    );
	    new distExports.ContentArea($placeholder[0], controller, commonMessages, {
	      enableAccessibility: options.enableAccessibility,
	      initialPageAreaImageUrl: options.initialPageAreaImageUrl
	    });
	    perspectiveManager = new PerspectiveManager(dom, controller, notificationService);
	    perspectiveManager.attach();
	    enableTouch($placeholder.find(".trv-page-container"));
	    initSplitter();
	    attachEvents();
	    attachEventHandlers();
	    initFromStorage();
	    initAccessibility(options);
	  }
	  function enableTouch(dom2) {
	    var allowSwipeLeft;
	    var allowSwipeRight;
	    function isMobileDevice() {
	      return /Mobile|Android|webOS|iPhone|iPad|iPod|BlackBerry|Opera Mini|Windows Phone/i.test(navigator.userAgent) || /Macintosh/i.test(navigator.userAgent) && navigator.maxTouchPoints && navigator.maxTouchPoints > 0;
	    }
	    TouchBehavior(
	      dom2,
	      {
	        swipe: function(e) {
	          var pageNumber = controller.getCurrentPageNumber();
	          if (allowSwipeLeft && e.direction === "left") {
	            if (pageNumber < controller.getPageCount()) {
	              controller.navigateToPage(pageNumber + 1);
	            }
	          } else if (allowSwipeRight && e.direction === "right") {
	            if (pageNumber > 1) {
	              controller.navigateToPage(pageNumber - 1);
	            }
	          }
	        },
	        pinch: function(e) {
	          var scale = controller.getScale();
	          var f = e.distance / e.lastDistance;
	          controller.setScaleMode(distExports.ScaleMode.Specific);
	          controller.setScale(scale * f);
	        },
	        doubletap: function() {
	          if (isMobileDevice()) {
	            commands.toggleZoomMode.exec();
	          }
	        },
	        touchstart: function() {
	          var el = dom2.find(".trv-page-wrapper")[0];
	          allowSwipeRight = 0 === el.scrollLeft;
	          allowSwipeLeft = el.scrollWidth - el.offsetWidth === el.scrollLeft;
	        }
	      }
	    );
	  }
	  function initSplitter() {
	    var parameterAreaPaneOptions = {
	      max: "500px",
	      min: "50px",
	      size: "300",
	      collapsible: true,
	      scrollable: false
	    };
	    var parameterAreaTemplate = $placeholder.find(".trv-parameters-area");
	    var parameterAreaPanes = [{}];
	    var documentMapPaneOptions = {
	      max: "500px",
	      min: "50px",
	      size: "300",
	      collapsible: true,
	      collapsed: true,
	      scrollable: false
	    };
	    var documentMapTemplate = $placeholder.find(".trv-document-map");
	    var documentMapPanes = [{}];
	    var orientation = "horizontal";
	    if (options.documentMapAreaPosition === DocumentMapAreaPositions.RIGHT) {
	      documentMapTemplate.insertAfter($placeholder.find(".trv-pages-area"));
	      documentMapPanes.push(documentMapPaneOptions);
	    } else {
	      documentMapPanes.unshift(documentMapPaneOptions);
	    }
	    if (options.parametersAreaPosition === ParametersAreaPositions.TOP || options.parametersAreaPosition === ParametersAreaPositions.BOTTOM) {
	      orientation = "vertical";
	      parameterAreaTemplate.addClass("-vertical");
	      parameterAreaPaneOptions.size = "130px";
	    }
	    if (options.parametersAreaPosition === ParametersAreaPositions.LEFT || options.parametersAreaPosition === ParametersAreaPositions.TOP) {
	      parameterAreaTemplate.insertBefore($placeholder.find(".trv-document-map-splitter"));
	      parameterAreaPanes.unshift(parameterAreaPaneOptions);
	    } else {
	      parameterAreaPanes.push(parameterAreaPaneOptions);
	    }
	    try {
	      $placeholder.find(".trv-document-map-splitter").attr("id", options.viewerSelector + "-document-map-splitter").kendoSplitter({
	        panes: documentMapPanes,
	        expand: (e) => {
	          setSplitterPaneVisibility(e.pane, true);
	        },
	        collapse: (e) => {
	          setSplitterPaneVisibility(e.pane, false);
	        },
	        resize: (e) => {
	        }
	      }).data("kendoSplitter");
	    } catch (e) {
	      console.error("Instantiation of Kendo Splitter as Document Map splitter threw an exception", e);
	      throw e;
	    }
	    try {
	      $placeholder.find(".trv-parameters-splitter").attr("id", options.viewerSelector + "-parameters-splitter").kendoSplitter({
	        panes: parameterAreaPanes,
	        orientation,
	        expand: (e) => {
	          setSplitterPaneVisibility(e.pane, true);
	        },
	        collapse: (e) => {
	          setSplitterPaneVisibility(e.pane, false);
	        },
	        resize: (e) => {
	        }
	      }).data("kendoSplitter");
	    } catch (e) {
	      console.error("Instantiation of Kendo Splitter as Parameters area splitter threw an exception", e);
	      throw e;
	    }
	  }
	  function setSplitterPaneVisibility(pane, visible) {
	    var paneID = $(pane).attr("data-id");
	    switch (paneID) {
	      case "trv-document-map":
	        notificationService.setDocumentMapVisible({
	          visible
	        });
	        break;
	      case "trv-parameters-area":
	        notificationService.setParametersAreaVisible({
	          visible
	        });
	        break;
	    }
	  }
	  function initFromStorage() {
	    var vm = settings.getViewMode();
	    var psm = settings.getPageMode();
	    var pm = settings.getPrintMode();
	    var s = settings.getScale();
	    var sm = settings.getScaleMode();
	    var dm = settings.getDocumentMapVisible();
	    var pa = settings.getParametersAreaVisible();
	    settings.getAccessibilityKeyMap();
	    controller.setViewMode(viewModeMap[vm ? vm : options.viewMode]);
	    controller.setPageMode(pageModeMap[psm ? psm : options.pageMode]);
	    controller.setPrintMode(printModeMap[pm ? pm : options.printMode]);
	    controller.setScaleMode(scaleModeMap[sm ? sm : options.scaleMode]);
	    controller.setScale(s ? s : options.scale);
	    notificationService.setDocumentMapVisible({
	      visible: dm ? dm : options.documentMapVisible
	    });
	    notificationService.setParametersAreaVisible({
	      visible: pa ? pa : options.parametersAreaVisible
	    });
	    controller.on("viewModeChanged", () => {
	      settings.setViewMode(viewModeReverseMap[controller.getViewMode()]);
	    }).on("pageModeChanged", () => {
	      settings.setPageMode(pageModeReverseMap[controller.getPageMode()]);
	    }).on("printModeChanged", () => {
	      settings.setPrintMode(printModeReverseMap[controller.getPrintMode()]);
	    }).on("scaleModeChanged", (scaleMode) => {
	      settings.setScaleMode(scaleModeReverseMap[scaleMode]);
	    }).on("scaleChanged", (scale) => {
	      settings.setScale(scale);
	    }).on("clientIdChanged", (clientId) => {
	      settings.setClientId(clientId);
	    }).on("currentPageChanged", (args) => {
	      settings.setPageNumber(args.page);
	    }).on("reportSourceChanged", (rs) => {
	      settings.setReportSource(rs);
	    });
	    notificationService.setDocumentMapVisible(() => {
	      var args = {};
	      notificationService.getDocumentMapState(args);
	      settings.setDocumentMapVisible(args.visible);
	    });
	    notificationService.setParametersAreaVisible(() => {
	      var args = {};
	      notificationService.getParametersAreaState(args);
	      settings.setParametersAreaVisible(args.visible);
	    });
	  }
	  function initAccessibility(options2) {
	    if (options2.enableAccessibility) {
	      accessibility = new Accessibility({
	        controller,
	        notificationService,
	        templates
	      });
	      var am = options2.accessibilityKeyMap;
	      if (am) {
	        accessibility.setKeyMap(am);
	      }
	      controller.setContentTabIndex(getTemplateContentTabIndex());
	    }
	  }
	  function getTemplateContentTabIndex() {
	    var pageAreaSelector = "div.trv-pages-area";
	    try {
	      var $pagesArea = $placeholder.find(pageAreaSelector);
	      if ($pagesArea.length === 0) {
	        throw "Selector " + pageAreaSelector + " did not return a result.";
	      }
	      return parseInt($pagesArea.attr("tabindex"));
	    } catch (e) {
	      if (console)
	        console.log(e);
	      return 0;
	    }
	  }
	  function start() {
	    var pendingRefresh = false;
	    init();
	    if (!(options.webDesignerPreview || options.serverPreview)) {
	      if (controller.shouldShowLicenseBanner()) {
	        $(".trv-content-wrapper")?.prepend('<span class="trv-license-banner"></span>');
	        const licenseBanner = $(".trv-license-banner").kendoNotification({
	          appendTo: ".trv-license-banner",
	          hideOnClick: false,
	          autoHideAfter: 0,
	          button: true,
	          hide: () => {
	            controller.saveToSessionStorage("hideBanner", "true");
	          }
	        }).data("kendoNotification");
	        const licenseData = controller.getConfigurationInfo()?.license;
	        licenseBanner.show(`<span class='trv-license-message'>${licenseData?.title || licenseData?.message} ${licenseData?.actionMessage}</span>
                                    <a class='trv-license-link' target='_blank' href='${licenseData?.actionLink}'>
                                        <span class='k-icon k-i-question-circle'></span>
                                    </a>`, "warning");
	      }
	      if (controller.shouldShowLicenseOverlay()) {
	        $(".trv-content-wrapper")?.prepend('<div class="trv-license-overlay"></div>');
	      }
	    }
	    controller.onAsync("reportLoadComplete", async () => {
	      if (options.documentMapVisible === false) {
	        notificationService.setDocumentMapVisible({ visible: false });
	      }
	    }).on("navigateToReport", (args) => {
	      controller.setReportSource(args);
	      controller.refreshReport(false);
	    }).on("toolTipOpening", (args) => {
	      showTooltip(args);
	    }).on("toolTipClosing", (args) => {
	      hideTooltip(args);
	    });
	    var rs = settings.getReportSource();
	    if (rs !== void 0) {
	      controller.setReportSource(rs);
	      var pageNumber = settings.getPageNumber();
	      if (pageNumber !== void 0) {
	        controller.navigateToPage(pageNumber);
	      }
	      pendingRefresh = true;
	    } else {
	      if (options.viewMode) {
	        controller.setViewMode(viewModeMap[options.viewMode]);
	      }
	      if (options.pageMode) {
	        controller.setPageMode(pageModeMap[options.pageMode]);
	      }
	      if (options.reportSource) {
	        controller.setReportSource(options.reportSource);
	        pendingRefresh = true;
	      }
	    }
	    if (typeof options.ready === "function") {
	      options.ready.call(viewer);
	    }
	    if (pendingRefresh) {
	      controller.refreshReport(false, "", true);
	    }
	  }
	  function showTooltip(args) {
	    var $element = $(args.element);
	    var toolTipArgs = {
	      element: args.element,
	      toolTip: {
	        title: args.title,
	        text: args.text
	      },
	      cancel: false
	    };
	    clearTooltipTimeout();
	    $(viewer).trigger({ type: Events$1.VIEWER_TOOLTIP_OPENING }, toolTipArgs);
	    if (toolTipArgs.cancel) {
	      return;
	    }
	    var content = applyToolTipTemplate(toolTipArgs);
	    var viewportElement = args.element.viewportElement;
	    var ktt = getToolTip($element, content);
	    viewer._tooltipShowTimeout = setTimeout(() => {
	      ktt.show($element);
	      if (viewportElement && viewportElement.nodeName === "svg") {
	        positionToolTip(ktt, args);
	      }
	    }, TOOLIP_SHOW_TIMEOUT);
	  }
	  function hideTooltip(args) {
	    var $t = $(args.element);
	    var toolTip = $t.data("kendoTooltip");
	    clearTooltipTimeout();
	    if (toolTip) {
	      toolTip.hide();
	    }
	  }
	  function clearTooltipTimeout() {
	    if (viewer._tooltipShowTimeout) {
	      clearTimeout(viewer._tooltipShowTimeout);
	      viewer._tooltipShowTimeout = null;
	    }
	  }
	  function applyToolTipTemplate(toolTipArgs) {
	    var toolTipTemplate = templates["trv-pages-area-kendo-tooltip"];
	    var $container = $(toolTipTemplate);
	    var $titleSpan = $container.find(".trv-pages-area-kendo-tooltip-title");
	    var $textSpan = $container.find(".trv-pages-area-kendo-tooltip-text");
	    $titleSpan.text(toolTipArgs.toolTip.title);
	    $textSpan.text(toolTipArgs.toolTip.text);
	    return $container.clone().wrap("<p>").parent().html();
	  }
	  function getToolTip(target, toolTipContent) {
	    var ktt = target.data("kendoTooltip");
	    if (!ktt) {
	      try {
	        ktt = target.kendoTooltip({
	          content: toolTipContent,
	          autohide: true,
	          callout: true,
	          position: "top"
	        }).data("kendoTooltip");
	      } catch (e) {
	        console.error("Instantiation of Kendo Tooltip threw an exception", e);
	        throw e;
	      }
	    }
	    return ktt;
	  }
	  function positionToolTip(toolTip, e) {
	    var x = e.pageX;
	    var y = e.pageY;
	    toolTip.popup.element.parent().css({
	      left: x + 10,
	      top: y + 5
	    });
	  }
	  function loadStyleSheets(styleSheets) {
	    if (!styleSheets)
	      return Promise.resolve();
	    var $head = $("head");
	    var currentStyleLinks = $head.find("link").map((i, e) => {
	      return e.outerHTML;
	    }).toArray();
	    var promises = [];
	    Array.from(styleSheets).forEach((element) => {
	      if (currentStyleLinks.indexOf(element) === -1) {
	        promises.push(
	          new Promise((resolve, reject) => {
	            var $link = $(element);
	            $link.on("load", resolve);
	            $link.on("onerror", () => {
	              logError("error loading stylesheet " + element);
	              resolve();
	            });
	            $head.append($link);
	          })
	        );
	      }
	    });
	    return Promise.all(promises).then(notificationService.cssLoaded.bind(notificationService));
	  }
	  function browserSupportsAllFeatures() {
	    return window.Promise;
	  }
	  function ensureKendo(version2) {
	    if (window.kendo) {
	      return Promise.resolve();
	    }
	    var kendoUrl = rTrim(svcApiUrl, "\\/") + "/resources/js/telerikReportViewer.kendo-" + version2 + ".min.js/";
	    return fetch(kendoUrl).then((response) => {
	      if (!response.ok) {
	        return Promise.reject({ error: "Failed to fetch data - status code " + response.status });
	      }
	      return response.text();
	    }).then((kendoScript) => {
	      const scriptElement = document.createElement("script");
	      scriptElement.textContent = kendoScript;
	      document.head.appendChild(scriptElement);
	    }).catch((errorData) => {
	      logError("Kendo could not be loaded automatically. Make sure 'options.serviceUrl' / 'options.reportServer.url' is correct and accessible. The error is: " + errorData.error);
	    });
	  }
	  function main(version2) {
	    ensureKendo(version2).then(() => {
	      viewer.authenticationToken(options.authenticationToken);
	      controller.getServiceConfiguration().catch((ex) => {
	        var errorOutput = isApplicationExceptionInstance(ex) ? ex.exceptionMessage : stringFormat(stringResources.errorServiceUrl, [escapeHtml(svcApiUrl)]);
	        $placeholder.text(errorOutput);
	        return Promise.reject(errorOutput);
	      }).then((configurationInfo) => {
	        controller.setConfigurationInfo(configurationInfo);
	        if (configurationInfo.version !== version2) {
	          var errorOutput = stringFormat(stringResources.errorServiceVersion, [configurationInfo.version, version2]);
	          $placeholder.text(errorOutput);
	          return Promise.reject(errorOutput);
	        }
	        TemplateCache.load(options.templateUrl, svcApiUrl, client).catch(() => {
	          var errorOutput2 = stringFormat(stringResources.errorLoadingTemplates, [escapeHtml(options.templateUrl)]);
	          $placeholder.text(errorOutput2);
	          return Promise.reject(errorOutput2);
	        }).then((result) => {
	          templates = result.templates;
	          return loadStyleSheets(result.styleSheets);
	        }).then(start);
	      }).catch((reason) => {
	        $(viewer).trigger({
	          type: Events$1.ERROR,
	          data: { sender: viewer }
	        }, reason);
	      });
	    });
	  }
	  if (browserSupportsAllFeatures()) {
	    main(version);
	  } else {
	    throw "The current browser does not support the Promise feature which is required for using the Report Viewer.";
	  }
	  return viewer;
	}
	function ReportViewerServiceClient(serviceClientOptions) {
	  return new distExports.ServiceClient(serviceClientOptions);
	}
	function ReportViewerController(controllerOptions) {
	  return new distExports.ReportController(controllerOptions?.serviceClient, controllerOptions?.settings);
	}
	function ReportViewerNotificationService() {
	  return new NotificationService();
	}
	var pluginName = "telerik_ReportViewer";
	$.fn[pluginName] = function(options) {
	  if (this.selector && !options.selector) {
	    options.selector = this.selector;
	  }
	  return this.each(function() {
	    if (!$.data(this, pluginName)) {
	      $.data(this, pluginName, new ReportViewer(this, options));
	    }
	  });
	};
	const plugins = [
	  {
	    name: "telerik_ReportViewer_DocumentMapArea",
	    constructor: DocumentMapArea
	  },
	  {
	    name: "telerik_ReportViewer_ParametersArea",
	    constructor: ParametersArea
	  },
	  {
	    name: "telerik_ReportViewer_SearchDialog",
	    constructor: Search
	  },
	  {
	    name: "telerik_ReportViewer_SendEmail",
	    constructor: SendEmail
	  },
	  {
	    name: "telerik_ReportViewer_LinkButton",
	    constructor: LinkButton
	  },
	  {
	    name: "telerik_ReportViewer_PageNumberInput",
	    constructor: PageNumberInput
	  },
	  {
	    name: "telerik_ReportViewer_PageCountLabel",
	    constructor: PageCountLabel
	  },
	  {
	    name: "telerik_ReportViewer_Toolbar",
	    constructor: Toolbar
	  },
	  {
	    name: "telerik_ReportViewer_AiPromptDialog",
	    constructor: AiPrompt
	  }
	];
	plugins.forEach((plugin) => {
	  $.fn[plugin.name] = function(options, otherOptions) {
	    return this.each(function() {
	      if (!$.data(this, plugin.name)) {
	        $.data(this, plugin.name, new plugin.constructor(this, options, otherOptions));
	      }
	    });
	  };
	});

	exports.Accessibility = Accessibility;
	exports.Binder = Binder;
	exports.CommandSet = CommandSet;
	exports.DocumentMapAreaPositions = DocumentMapAreaPositions;
	exports.Events = Events$1;
	exports.GlobalSettings = GlobalSettings;
	exports.HistoryManager = HistoryManager;
	exports.MemStorage = MemStorage;
	exports.PageModes = PageModes;
	exports.ParameterEditorTypes = ParameterEditorTypes;
	exports.ParameterEditors = ParameterEditors;
	exports.ParameterTypes = ParameterTypes;
	exports.ParameterValidators = ParameterValidators;
	exports.ParametersArea = ParametersArea;
	exports.ParametersAreaPositions = ParametersAreaPositions;
	exports.PerspectiveManager = PerspectiveManager;
	exports.PrintModes = PrintModes;
	exports.ReportViewer = ReportViewer;
	exports.ReportViewerController = ReportViewerController;
	exports.ReportViewerNotificationService = ReportViewerNotificationService;
	exports.ReportViewerServiceClient = ReportViewerServiceClient;
	exports.ReportViewerSettings = ReportViewerSettings;
	exports.ScaleModes = ScaleModes;
	exports.TouchBehavior = TouchBehavior;
	exports.UIController = UIController;
	exports.ViewModes = ViewModes;
	exports.domUtils = domUtils;
	exports.parameterEditorsMatch = parameterEditorsMatch;
	exports.utils = utils;

	Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });

	return exports;

})({});
