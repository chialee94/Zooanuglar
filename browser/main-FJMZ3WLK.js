var Pg=Object.defineProperty,Lg=Object.defineProperties;var Vg=Object.getOwnPropertyDescriptors;var Pd=Object.getOwnPropertySymbols;var jg=Object.prototype.hasOwnProperty,Bg=Object.prototype.propertyIsEnumerable;var Ld=(e,t,n)=>t in e?Pg(e,t,{enumerable:!0,configurable:!0,writable:!0,value:n}):e[t]=n,I=(e,t)=>{for(var n in t||={})jg.call(t,n)&&Ld(e,n,t[n]);if(Pd)for(var n of Pd(t))Bg.call(t,n)&&Ld(e,n,t[n]);return e},P=(e,t)=>Lg(e,Vg(t));var fe=null,Io=!1,Qs=1,Hg=null,ie=Symbol("SIGNAL");function w(e){let t=fe;return fe=e,t}function So(){return fe}var Wt={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function In(e){if(Io)throw new Error("");if(fe===null)return;fe.consumerOnSignalRead(e);let t=fe.producersTail;if(t!==void 0&&t.producer===e)return;let n,r=fe.recomputing;if(r&&(n=t!==void 0?t.nextProducer:fe.producers,n!==void 0&&n.producer===e)){fe.producersTail=n,n.lastReadVersion=e.version;return}let o=e.consumersTail;if(o!==void 0&&o.consumer===fe&&(!r||$g(o,fe)))return;let i=Mn(fe),s={producer:e,consumer:fe,nextProducer:n,prevConsumer:o,lastReadVersion:e.version,nextConsumer:void 0};fe.producersTail=s,t!==void 0?t.nextProducer=s:fe.producers=s,i&&Hd(e,s)}function Vd(){Qs++}function Ao(e){if(!(Mn(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===Qs)){if(!e.producerMustRecompute(e)&&!xn(e)){To(e);return}e.producerRecomputeValue(e),To(e)}}function Ks(e){if(e.consumers===void 0)return;let t=Io;Io=!0;try{for(let n=e.consumers;n!==void 0;n=n.nextConsumer){let r=n.consumer;r.dirty||Ug(r)}}finally{Io=t}}function Xs(){return fe?.consumerAllowSignalWrites!==!1}function Ug(e){e.dirty=!0,Ks(e),e.consumerMarkedDirty?.(e)}function To(e){e.dirty=!1,e.lastCleanEpoch=Qs}function Dt(e){return e&&jd(e),w(e)}function jd(e){e.producersTail=void 0,e.recomputing=!0}function qt(e,t){w(t),e&&Bd(e)}function Bd(e){e.recomputing=!1;let t=e.producersTail,n=t!==void 0?t.nextProducer:e.producers;if(n!==void 0){if(Mn(e))do n=Js(n);while(n!==void 0);t!==void 0?t.nextProducer=void 0:e.producers=void 0}}function xn(e){for(let t=e.producers;t!==void 0;t=t.nextProducer){let n=t.producer,r=t.lastReadVersion;if(r!==n.version||(Ao(n),r!==n.version))return!0}return!1}function Et(e){if(Mn(e)){let t=e.producers;for(;t!==void 0;)t=Js(t)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function Hd(e,t){let n=e.consumersTail,r=Mn(e);if(n!==void 0?(t.nextConsumer=n.nextConsumer,n.nextConsumer=t):(t.nextConsumer=void 0,e.consumers=t),t.prevConsumer=n,e.consumersTail=t,!r)for(let o=e.producers;o!==void 0;o=o.nextProducer)Hd(o.producer,o)}function Js(e){let t=e.producer,n=e.nextProducer,r=e.nextConsumer,o=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,r!==void 0?r.prevConsumer=o:t.consumersTail=o,o!==void 0)o.nextConsumer=r;else if(t.consumers=r,!Mn(t)){let i=t.producers;for(;i!==void 0;)i=Js(i)}return n}function Mn(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function No(e){Hg?.(e)}function $g(e,t){let n=t.producersTail;if(n!==void 0){let r=t.producers;do{if(r===e)return!0;if(r===n)break;r=r.nextProducer}while(r!==void 0)}return!1}function Ro(e,t){return Object.is(e,t)}function _r(e,t){let n=Object.create(zg);n.computation=e,t!==void 0&&(n.equal=t);let r=()=>{if(Ao(n),In(n),n.value===br)throw n.error;return n.value};return r[ie]=n,No(n),r}var xo=Symbol("UNSET"),Mo=Symbol("COMPUTING"),br=Symbol("ERRORED"),zg=P(I({},Wt),{value:xo,dirty:!0,error:null,equal:Ro,kind:"computed",producerMustRecompute(e){return e.value===xo||e.value===Mo},producerRecomputeValue(e){if(e.value===Mo)throw new Error("");let t=e.value;e.value=Mo;let n=Dt(e),r,o=!1;try{r=e.computation(),w(null),o=t!==xo&&t!==br&&r!==br&&e.equal(t,r)}catch(i){r=br,e.error=i}finally{qt(e,n)}if(o){e.value=t;return}e.value=r,e.version++}});function Gg(){throw new Error}var Ud=Gg;function $d(e){Ud(e)}function ea(e){Ud=e}var Wg=null;function ta(e,t){let n=Object.create(Fo);n.value=e,t!==void 0&&(n.equal=t);let r=()=>zd(n);return r[ie]=n,No(n),[r,s=>Dr(n,s),s=>na(n,s)]}function zd(e){return In(e),e.value}function Dr(e,t){Xs()||$d(e),e.equal(e.value,t)||(e.value=t,qg(e))}function na(e,t){Xs()||$d(e),Dr(e,t(e.value))}var Fo=P(I({},Wt),{equal:Ro,value:void 0,kind:"signal"});function qg(e){e.version++,Vd(),Ks(e),Wg?.(e)}var ra=P(I({},Wt),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function oa(e){if(e.dirty=!1,e.version>0&&!xn(e))return;e.version++;let t=Dt(e);try{e.cleanup(),e.fn()}finally{qt(e,t)}}function x(e){return typeof e=="function"}function Oo(e){let n=e(r=>{Error.call(r),r.stack=new Error().stack});return n.prototype=Object.create(Error.prototype),n.prototype.constructor=n,n}var ko=Oo(e=>function(n){e(this),this.message=n?`${n.length} errors occurred during unsubscription:
${n.map((r,o)=>`${o+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=n});function Er(e,t){if(e){let n=e.indexOf(t);0<=n&&e.splice(n,1)}}var se=class e{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:n}=this;if(n)if(this._parentage=null,Array.isArray(n))for(let i of n)i.remove(this);else n.remove(this);let{initialTeardown:r}=this;if(x(r))try{r()}catch(i){t=i instanceof ko?i.errors:[i]}let{_finalizers:o}=this;if(o){this._finalizers=null;for(let i of o)try{Gd(i)}catch(s){t=t??[],s instanceof ko?t=[...t,...s.errors]:t.push(s)}}if(t)throw new ko(t)}}add(t){var n;if(t&&t!==this)if(this.closed)Gd(t);else{if(t instanceof e){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(n=this._finalizers)!==null&&n!==void 0?n:[]).push(t)}}_hasParent(t){let{_parentage:n}=this;return n===t||Array.isArray(n)&&n.includes(t)}_addParent(t){let{_parentage:n}=this;this._parentage=Array.isArray(n)?(n.push(t),n):n?[n,t]:t}_removeParent(t){let{_parentage:n}=this;n===t?this._parentage=null:Array.isArray(n)&&Er(n,t)}remove(t){let{_finalizers:n}=this;n&&Er(n,t),t instanceof e&&t._removeParent(this)}};se.EMPTY=(()=>{let e=new se;return e.closed=!0,e})();var ia=se.EMPTY;function Po(e){return e instanceof se||e&&"closed"in e&&x(e.remove)&&x(e.add)&&x(e.unsubscribe)}function Gd(e){x(e)?e():e.unsubscribe()}var ke={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Tn={setTimeout(e,t,...n){let{delegate:r}=Tn;return r?.setTimeout?r.setTimeout(e,t,...n):setTimeout(e,t,...n)},clearTimeout(e){let{delegate:t}=Tn;return(t?.clearTimeout||clearTimeout)(e)},delegate:void 0};function Lo(e){Tn.setTimeout(()=>{let{onUnhandledError:t}=ke;if(t)t(e);else throw e})}function Cr(){}var Wd=sa("C",void 0,void 0);function qd(e){return sa("E",void 0,e)}function Zd(e){return sa("N",e,void 0)}function sa(e,t,n){return{kind:e,value:t,error:n}}var Zt=null;function Sn(e){if(ke.useDeprecatedSynchronousErrorHandling){let t=!Zt;if(t&&(Zt={errorThrown:!1,error:null}),e(),t){let{errorThrown:n,error:r}=Zt;if(Zt=null,n)throw r}}else e()}function Yd(e){ke.useDeprecatedSynchronousErrorHandling&&Zt&&(Zt.errorThrown=!0,Zt.error=e)}var Yt=class extends se{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,Po(t)&&t.add(this)):this.destination=Qg}static create(t,n,r){return new dt(t,n,r)}next(t){this.isStopped?ca(Zd(t),this):this._next(t)}error(t){this.isStopped?ca(qd(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?ca(Wd,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Zg=Function.prototype.bind;function aa(e,t){return Zg.call(e,t)}var la=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:n}=this;if(n.next)try{n.next(t)}catch(r){Vo(r)}}error(t){let{partialObserver:n}=this;if(n.error)try{n.error(t)}catch(r){Vo(r)}else Vo(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(n){Vo(n)}}},dt=class extends Yt{constructor(t,n,r){super();let o;if(x(t)||!t)o={next:t??void 0,error:n??void 0,complete:r??void 0};else{let i;this&&ke.useDeprecatedNextContext?(i=Object.create(t),i.unsubscribe=()=>this.unsubscribe(),o={next:t.next&&aa(t.next,i),error:t.error&&aa(t.error,i),complete:t.complete&&aa(t.complete,i)}):o=t}this.destination=new la(o)}};function Vo(e){ke.useDeprecatedSynchronousErrorHandling?Yd(e):Lo(e)}function Yg(e){throw e}function ca(e,t){let{onStoppedNotification:n}=ke;n&&Tn.setTimeout(()=>n(e,t))}var Qg={closed:!0,next:Cr,error:Yg,complete:Cr};var An=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Ct(e){return e}function Qd(e){return e.length===0?Ct:e.length===1?e[0]:function(n){return e.reduce((r,o)=>o(r),n)}}var F=(()=>{class e{constructor(n){n&&(this._subscribe=n)}lift(n){let r=new e;return r.source=this,r.operator=n,r}subscribe(n,r,o){let i=Xg(n)?n:new dt(n,r,o);return Sn(()=>{let{operator:s,source:a}=this;i.add(s?s.call(i,a):a?this._subscribe(i):this._trySubscribe(i))}),i}_trySubscribe(n){try{return this._subscribe(n)}catch(r){n.error(r)}}forEach(n,r){return r=Kd(r),new r((o,i)=>{let s=new dt({next:a=>{try{n(a)}catch(c){i(c),s.unsubscribe()}},error:i,complete:o});this.subscribe(s)})}_subscribe(n){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(n)}[An](){return this}pipe(...n){return Qd(n)(this)}toPromise(n){return n=Kd(n),new n((r,o)=>{let i;this.subscribe(s=>i=s,s=>o(s),()=>r(i))})}}return e.create=t=>new e(t),e})();function Kd(e){var t;return(t=e??ke.Promise)!==null&&t!==void 0?t:Promise}function Kg(e){return e&&x(e.next)&&x(e.error)&&x(e.complete)}function Xg(e){return e&&e instanceof Yt||Kg(e)&&Po(e)}function Jg(e){return x(e?.lift)}function L(e){return t=>{if(Jg(t))return t.lift(function(n){try{return e(n,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function z(e,t,n,r,o){return new da(e,t,n,r,o)}var da=class extends Yt{constructor(t,n,r,o,i,s){super(t),this.onFinalize=i,this.shouldUnsubscribe=s,this._next=n?function(a){try{n(a)}catch(c){t.error(c)}}:super._next,this._error=o?function(a){try{o(a)}catch(c){t.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:n}=this;super.unsubscribe(),!n&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};var Xd=Oo(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var G=(()=>{class e extends F{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let r=new jo(this,this);return r.operator=n,r}_throwIfClosed(){if(this.closed)throw new Xd}next(n){Sn(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(n)}})}error(n){Sn(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:r}=this;for(;r.length;)r.shift().error(n)}})}complete(){Sn(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:r,isStopped:o,observers:i}=this;return r||o?ia:(this.currentObservers=null,i.push(n),new se(()=>{this.currentObservers=null,Er(i,n)}))}_checkFinalizedStatuses(n){let{hasError:r,thrownError:o,isStopped:i}=this;r?n.error(o):i&&n.complete()}asObservable(){let n=new F;return n.source=this,n}}return e.create=(t,n)=>new jo(t,n),e})(),jo=class extends G{constructor(t,n){super(),this.destination=t,this.source=n}next(t){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.next)===null||r===void 0||r.call(n,t)}error(t){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.error)===null||r===void 0||r.call(n,t)}complete(){var t,n;(n=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||n===void 0||n.call(t)}_subscribe(t){var n,r;return(r=(n=this.source)===null||n===void 0?void 0:n.subscribe(t))!==null&&r!==void 0?r:ia}};var Qt=class extends G{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let n=super._subscribe(t);return!n.closed&&t.next(this._value),n}getValue(){let{hasError:t,thrownError:n,_value:r}=this;if(t)throw n;return this._throwIfClosed(),r}next(t){super.next(this._value=t)}};var ua={now(){return(ua.delegate||Date).now()},delegate:void 0};var Bo=class extends G{constructor(t=1/0,n=1/0,r=ua){super(),this._bufferSize=t,this._windowTime=n,this._timestampProvider=r,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=n===1/0,this._bufferSize=Math.max(1,t),this._windowTime=Math.max(1,n)}next(t){let{isStopped:n,_buffer:r,_infiniteTimeWindow:o,_timestampProvider:i,_windowTime:s}=this;n||(r.push(t),!o&&r.push(i.now()+s)),this._trimBuffer(),super.next(t)}_subscribe(t){this._throwIfClosed(),this._trimBuffer();let n=this._innerSubscribe(t),{_infiniteTimeWindow:r,_buffer:o}=this,i=o.slice();for(let s=0;s<i.length&&!t.closed;s+=r?1:2)t.next(i[s]);return this._checkFinalizedStatuses(t),n}_trimBuffer(){let{_bufferSize:t,_timestampProvider:n,_buffer:r,_infiniteTimeWindow:o}=this,i=(o?1:2)*t;if(t<1/0&&i<r.length&&r.splice(0,r.length-i),!o){let s=n.now(),a=0;for(let c=1;c<r.length&&r[c]<=s;c+=2)a=c;a&&r.splice(0,a+1)}}};var Jd=new F(e=>e.complete());function eu(e){return e&&x(e.schedule)}function fa(e){return e[e.length-1]}function tu(e){return x(fa(e))?e.pop():void 0}function wt(e){return eu(fa(e))?e.pop():void 0}function nu(e,t){return typeof fa(e)=="number"?e.pop():t}function ou(e,t,n,r){function o(i){return i instanceof n?i:new n(function(s){s(i)})}return new(n||(n=Promise))(function(i,s){function a(d){try{l(r.next(d))}catch(u){s(u)}}function c(d){try{l(r.throw(d))}catch(u){s(u)}}function l(d){d.done?i(d.value):o(d.value).then(a,c)}l((r=r.apply(e,t||[])).next())})}function ru(e){var t=typeof Symbol=="function"&&Symbol.iterator,n=t&&e[t],r=0;if(n)return n.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&r>=e.length&&(e=void 0),{value:e&&e[r++],done:!e}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function Kt(e){return this instanceof Kt?(this.v=e,this):new Kt(e)}function iu(e,t,n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=n.apply(e,t||[]),o,i=[];return o=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),o[Symbol.asyncIterator]=function(){return this},o;function s(f){return function(h){return Promise.resolve(h).then(f,u)}}function a(f,h){r[f]&&(o[f]=function(C){return new Promise(function(y,b){i.push([f,C,y,b])>1||c(f,C)})},h&&(o[f]=h(o[f])))}function c(f,h){try{l(r[f](h))}catch(C){p(i[0][3],C)}}function l(f){f.value instanceof Kt?Promise.resolve(f.value.v).then(d,u):p(i[0][2],f)}function d(f){c("next",f)}function u(f){c("throw",f)}function p(f,h){f(h),i.shift(),i.length&&c(i[0][0],i[0][1])}}function su(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=e[Symbol.asyncIterator],n;return t?t.call(e):(e=typeof ru=="function"?ru(e):e[Symbol.iterator](),n={},r("next"),r("throw"),r("return"),n[Symbol.asyncIterator]=function(){return this},n);function r(i){n[i]=e[i]&&function(s){return new Promise(function(a,c){s=e[i](s),o(a,c,s.done,s.value)})}}function o(i,s,a,c){Promise.resolve(c).then(function(l){i({value:l,done:a})},s)}}var Ho=e=>e&&typeof e.length=="number"&&typeof e!="function";function Uo(e){return x(e?.then)}function $o(e){return x(e[An])}function zo(e){return Symbol.asyncIterator&&x(e?.[Symbol.asyncIterator])}function Go(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function ev(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Wo=ev();function qo(e){return x(e?.[Wo])}function Zo(e){return iu(this,arguments,function*(){let n=e.getReader();try{for(;;){let{value:r,done:o}=yield Kt(n.read());if(o)return yield Kt(void 0);yield yield Kt(r)}}finally{n.releaseLock()}})}function Yo(e){return x(e?.getReader)}function W(e){if(e instanceof F)return e;if(e!=null){if($o(e))return tv(e);if(Ho(e))return nv(e);if(Uo(e))return rv(e);if(zo(e))return au(e);if(qo(e))return ov(e);if(Yo(e))return iv(e)}throw Go(e)}function tv(e){return new F(t=>{let n=e[An]();if(x(n.subscribe))return n.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function nv(e){return new F(t=>{for(let n=0;n<e.length&&!t.closed;n++)t.next(e[n]);t.complete()})}function rv(e){return new F(t=>{e.then(n=>{t.closed||(t.next(n),t.complete())},n=>t.error(n)).then(null,Lo)})}function ov(e){return new F(t=>{for(let n of e)if(t.next(n),t.closed)return;t.complete()})}function au(e){return new F(t=>{sv(e,t).catch(n=>t.error(n))})}function iv(e){return au(Zo(e))}function sv(e,t){var n,r,o,i;return ou(this,void 0,void 0,function*(){try{for(n=su(e);r=yield n.next(),!r.done;){let s=r.value;if(t.next(s),t.closed)return}}catch(s){o={error:s}}finally{try{r&&!r.done&&(i=n.return)&&(yield i.call(n))}finally{if(o)throw o.error}}t.complete()})}function Me(e,t,n,r=0,o=!1){let i=t.schedule(function(){n(),o?e.add(this.schedule(null,r)):this.unsubscribe()},r);if(e.add(i),!o)return i}function Qo(e,t=0){return L((n,r)=>{n.subscribe(z(r,o=>Me(r,e,()=>r.next(o),t),()=>Me(r,e,()=>r.complete(),t),o=>Me(r,e,()=>r.error(o),t)))})}function Ko(e,t=0){return L((n,r)=>{r.add(e.schedule(()=>n.subscribe(r),t))})}function cu(e,t){return W(e).pipe(Ko(t),Qo(t))}function lu(e,t){return W(e).pipe(Ko(t),Qo(t))}function du(e,t){return new F(n=>{let r=0;return t.schedule(function(){r===e.length?n.complete():(n.next(e[r++]),n.closed||this.schedule())})})}function uu(e,t){return new F(n=>{let r;return Me(n,t,()=>{r=e[Wo](),Me(n,t,()=>{let o,i;try{({value:o,done:i}=r.next())}catch(s){n.error(s);return}i?n.complete():n.next(o)},0,!0)}),()=>x(r?.return)&&r.return()})}function Xo(e,t){if(!e)throw new Error("Iterable cannot be null");return new F(n=>{Me(n,t,()=>{let r=e[Symbol.asyncIterator]();Me(n,t,()=>{r.next().then(o=>{o.done?n.complete():n.next(o.value)})},0,!0)})})}function fu(e,t){return Xo(Zo(e),t)}function pu(e,t){if(e!=null){if($o(e))return cu(e,t);if(Ho(e))return du(e,t);if(Uo(e))return lu(e,t);if(zo(e))return Xo(e,t);if(qo(e))return uu(e,t);if(Yo(e))return fu(e,t)}throw Go(e)}function ut(e,t){return t?pu(e,t):W(e)}function Nn(...e){let t=wt(e);return ut(e,t)}function wr(e,t){let n=x(e)?e:()=>e,r=o=>o.error(n());return new F(t?o=>t.schedule(r,0,o):r)}function pe(e,t){return L((n,r)=>{let o=0;n.subscribe(z(r,i=>{r.next(e.call(t,i,o++))}))})}var{isArray:av}=Array;function cv(e,t){return av(t)?e(...t):e(t)}function mu(e){return pe(t=>cv(e,t))}var{isArray:lv}=Array,{getPrototypeOf:dv,prototype:uv,keys:fv}=Object;function hu(e){if(e.length===1){let t=e[0];if(lv(t))return{args:t,keys:null};if(pv(t)){let n=fv(t);return{args:n.map(r=>t[r]),keys:n}}}return{args:e,keys:null}}function pv(e){return e&&typeof e=="object"&&dv(e)===uv}function gu(e,t){return e.reduce((n,r,o)=>(n[r]=t[o],n),{})}function vu(e,t,n,r,o,i,s,a){let c=[],l=0,d=0,u=!1,p=()=>{u&&!c.length&&!l&&t.complete()},f=C=>l<r?h(C):c.push(C),h=C=>{i&&t.next(C),l++;let y=!1;W(n(C,d++)).subscribe(z(t,b=>{o?.(b),i?f(b):t.next(b)},()=>{y=!0},void 0,()=>{if(y)try{for(l--;c.length&&l<r;){let b=c.shift();s?Me(t,s,()=>h(b)):h(b)}p()}catch(b){t.error(b)}}))};return e.subscribe(z(t,f,()=>{u=!0,p()})),()=>{a?.()}}function Rn(e,t,n=1/0){return x(t)?Rn((r,o)=>pe((i,s)=>t(r,i,o,s))(W(e(r,o))),n):(typeof t=="number"&&(n=t),L((r,o)=>vu(r,o,e,n)))}function Jo(e=1/0){return Rn(Ct,e)}function yu(){return Jo(1)}function pa(...e){return yu()(ut(e,wt(e)))}function ma(...e){let t=tu(e),{args:n,keys:r}=hu(e),o=new F(i=>{let{length:s}=n;if(!s){i.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;W(n[d]).subscribe(z(i,p=>{u||(u=!0,l--),a[d]=p},()=>c--,void 0,()=>{(!c||!u)&&(l||i.next(r?gu(r,a):a),i.complete())}))}});return t?o.pipe(mu(t)):o}function ha(...e){let t=wt(e),n=nu(e,1/0),r=e;return r.length?r.length===1?W(r[0]):Jo(n)(ut(r,t)):Jd}function ft(e,t){return L((n,r)=>{let o=0;n.subscribe(z(r,i=>e.call(t,i,o++)&&r.next(i)))})}function Fn(e){return L((t,n)=>{let r=null,o=!1,i;r=t.subscribe(z(n,void 0,void 0,s=>{i=W(e(s,Fn(e)(t))),r?(r.unsubscribe(),r=null,i.subscribe(n)):o=!0})),o&&(r.unsubscribe(),r=null,i.subscribe(n))})}function ga(e,t){return x(t)?Rn(e,t,1):Rn(e,1)}function va(e,t=Ct){return e=e??mv,L((n,r)=>{let o,i=!0;n.subscribe(z(r,s=>{let a=t(s);(i||!e(o,a))&&(i=!1,o=a,r.next(s))}))})}function mv(e,t){return e===t}function ei(e){return L((t,n)=>{try{t.subscribe(n)}finally{n.add(e)}})}function ya(){return L((e,t)=>{let n,r=!1;e.subscribe(z(t,o=>{let i=n;n=o,r&&t.next([i,o]),r=!0}))})}function bu(e={}){let{connector:t=()=>new G,resetOnError:n=!0,resetOnComplete:r=!0,resetOnRefCountZero:o=!0}=e;return i=>{let s,a,c,l=0,d=!1,u=!1,p=()=>{a?.unsubscribe(),a=void 0},f=()=>{p(),s=c=void 0,d=u=!1},h=()=>{let C=s;f(),C?.unsubscribe()};return L((C,y)=>{l++,!u&&!d&&p();let b=c=c??t();y.add(()=>{l--,l===0&&!u&&!d&&(a=ba(h,o))}),b.subscribe(y),!s&&l>0&&(s=new dt({next:J=>b.next(J),error:J=>{u=!0,p(),a=ba(f,n,J),b.error(J)},complete:()=>{d=!0,p(),a=ba(f,r),b.complete()}}),W(C).subscribe(s))})(i)}}function ba(e,t,...n){if(t===!0){e();return}if(t===!1)return;let r=new dt({next:()=>{r.unsubscribe(),e()}});return W(t(...n)).subscribe(r)}function _a(e,t,n){let r,o=!1;return e&&typeof e=="object"?{bufferSize:r=1/0,windowTime:t=1/0,refCount:o=!1,scheduler:n}=e:r=e??1/0,bu({connector:()=>new Bo(r,t,n),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:o})}function Da(e){return ft((t,n)=>e<=n)}function Ea(...e){let t=wt(e);return L((n,r)=>{(t?pa(e,n,t):pa(e,n)).subscribe(r)})}function Ca(e,t){return L((n,r)=>{let o=null,i=0,s=!1,a=()=>s&&!o&&r.complete();n.subscribe(z(r,c=>{o?.unsubscribe();let l=0,d=i++;W(e(c,d)).subscribe(o=z(r,u=>r.next(t?t(c,u,d,l++):u),()=>{o=null,a()}))},()=>{s=!0,a()}))})}function Xt(e){return L((t,n)=>{W(e).subscribe(z(n,()=>n.complete(),Cr)),!n.closed&&t.subscribe(n)})}function Ir(e,t,n){let r=x(e)||t||n?{next:e,error:t,complete:n}:e;return r?L((o,i)=>{var s;(s=r.subscribe)===null||s===void 0||s.call(r);let a=!0;o.subscribe(z(i,c=>{var l;(l=r.next)===null||l===void 0||l.call(r,c),i.next(c)},()=>{var c;a=!1,(c=r.complete)===null||c===void 0||c.call(r),i.complete()},c=>{var l;a=!1,(l=r.error)===null||l===void 0||l.call(r,c),i.error(c)},()=>{var c,l;a&&((c=r.unsubscribe)===null||c===void 0||c.call(r)),(l=r.finalize)===null||l===void 0||l.call(r)}))}):Ct}var wa;function ti(){return wa}function Ye(e){let t=wa;return wa=e,t}var _u=Symbol("NotFound");function On(e){return e===_u||e?.name==="\u0275NotFound"}function Du(e){let t=w(null);try{return e()}finally{w(t)}}var La="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",_=class extends Error{code;constructor(t,n){super(Rr(t,n)),this.code=t}};function hv(e){return`NG0${Math.abs(e)}`}function Rr(e,t){return`${hv(e)}${t?": "+t:""}`}function V(e){for(let t in e)if(e[t]===V)return t;throw Error("")}function xu(e,t){for(let n in t)t.hasOwnProperty(n)&&!e.hasOwnProperty(n)&&(e[n]=t[n])}function ci(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(ci).join(", ")}]`;if(e==null)return""+e;let t=e.overriddenName||e.name;if(t)return`${t}`;let n=e.toString();if(n==null)return""+n;let r=n.indexOf(`
`);return r>=0?n.slice(0,r):n}function li(e,t){return e?t?`${e} ${t}`:e:t||""}var gv=V({__forward_ref__:V});function Mt(e){return e.__forward_ref__=Mt,e}function ae(e){return Va(e)?e():e}function Va(e){return typeof e=="function"&&e.hasOwnProperty(gv)&&e.__forward_ref__===Mt}function v(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function U(e){return{providers:e.providers||[],imports:e.imports||[]}}function di(e){return vv(e,ui)}function vv(e,t){return e.hasOwnProperty(t)&&e[t]||null}function yv(e){let t=e?.[ui]??null;return t||null}function xa(e){return e&&e.hasOwnProperty(ri)?e[ri]:null}var ui=V({\u0275prov:V}),ri=V({\u0275inj:V}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(t,n){this._desc=t,this.\u0275prov=void 0,typeof n=="number"?this.__NG_ELEMENT_ID__=n:n!==void 0&&(this.\u0275prov=v({token:this,providedIn:n.providedIn||"root",factory:n.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function ja(e){return e&&!!e.\u0275providers}var Ba=V({\u0275cmp:V}),Ha=V({\u0275dir:V}),Ua=V({\u0275pipe:V});var Mr=V({\u0275fac:V}),on=V({__NG_ELEMENT_ID__:V}),Eu=V({__NG_ENV_ID__:V});function Tt(e){return za(e,"@Component"),e[Ba]||null}function $a(e){return za(e,"@Directive"),e[Ha]||null}function Mu(e){return za(e,"@Pipe"),e[Ua]||null}function za(e,t){if(e==null)throw new _(-919,!1)}function Ga(e){return typeof e=="string"?e:e==null?"":String(e)}var Tu=V({ngErrorCode:V}),bv=V({ngErrorMessage:V}),_v=V({ngTokenPath:V});function Wa(e,t){return Su("",-200,t)}function fi(e,t){throw new _(-201,!1)}function Su(e,t,n){let r=new _(t,e);return r[Tu]=t,r[bv]=e,n&&(r[_v]=n),r}function Dv(e){return e[Tu]}var Ma;function Au(){return Ma}function Ce(e){let t=Ma;return Ma=e,t}function qa(e,t,n){let r=di(e);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(n&8)return null;if(t!==void 0)return t;fi(e,"")}var Ev={},Jt=Ev,Cv="__NG_DI_FLAG__",Ta=class{injector;constructor(t){this.injector=t}retrieve(t,n){let r=en(n)||0;try{return this.injector.get(t,r&8?null:Jt,r)}catch(o){if(On(o))return o;throw o}}};function wv(e,t=0){let n=ti();if(n===void 0)throw new _(-203,!1);if(n===null)return qa(e,void 0,t);{let r=Iv(t),o=n.retrieve(e,r);if(On(o)){if(r.optional)return null;throw o}return o}}function M(e,t=0){return(Au()||wv)(ae(e),t)}function m(e,t){return M(e,en(t))}function en(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function Iv(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function Sa(e){let t=[];for(let n=0;n<e.length;n++){let r=ae(e[n]);if(Array.isArray(r)){if(r.length===0)throw new _(900,!1);let o,i=0;for(let s=0;s<r.length;s++){let a=r[s],c=xv(a);typeof c=="number"?c===-1?o=a.token:i|=c:o=a}t.push(M(o,i))}else t.push(M(r))}return t}function xv(e){return e[Cv]}function tn(e,t){let n=e.hasOwnProperty(Mr);return n?e[Mr]:null}function Nu(e,t,n){if(e.length!==t.length)return!1;for(let r=0;r<e.length;r++){let o=e[r],i=t[r];if(n&&(o=n(o),i=n(i)),i!==o)return!1}return!0}function Ru(e){return e.flat(Number.POSITIVE_INFINITY)}function pi(e,t){e.forEach(n=>Array.isArray(n)?pi(n,t):t(n))}function Za(e,t,n){t>=e.length?e.push(n):e.splice(t,0,n)}function Fr(e,t){return t>=e.length-1?e.pop():e.splice(t,1)[0]}function Fu(e,t){let n=[];for(let r=0;r<e;r++)n.push(t);return n}function Ou(e,t,n,r){let o=e.length;if(o==t)e.push(n,r);else if(o===1)e.push(r,e[0]),e[0]=n;else{for(o--,e.push(e[o-1],e[o]);o>t;){let i=o-2;e[o]=e[i],o--}e[t]=n,e[t+1]=r}}function mi(e,t,n){let r=Pn(e,t);return r>=0?e[r|1]=n:(r=~r,Ou(e,r,t,n)),r}function hi(e,t){let n=Pn(e,t);if(n>=0)return e[n|1]}function Pn(e,t){return Mv(e,t,1)}function Mv(e,t,n){let r=0,o=e.length>>n;for(;o!==r;){let i=r+(o-r>>1),s=e[i<<n];if(t===s)return i<<n;s>t?o=i:r=i+1}return~(o<<n)}var St={},me=[],Or=new g(""),Ya=new g("",-1),Qa=new g(""),Tr=class{get(t,n=Jt){if(n===Jt){let o=Su("",-201);throw o.name="\u0275NotFound",o}return n}};function gi(e){return{\u0275providers:e}}function vi(...e){return{\u0275providers:Ka(!0,e),\u0275fromNgModule:!0}}function Ka(e,...t){let n=[],r=new Set,o,i=s=>{n.push(s)};return pi(t,s=>{let a=s;oi(a,i,[],r)&&(o||=[],o.push(a))}),o!==void 0&&ku(o,i),n}function ku(e,t){for(let n=0;n<e.length;n++){let{ngModule:r,providers:o}=e[n];Xa(o,i=>{t(i,r)})}}function oi(e,t,n,r){if(e=ae(e),!e)return!1;let o=null,i=xa(e),s=!i&&Tt(e);if(!i&&!s){let c=e.ngModule;if(i=xa(c),i)o=c;else return!1}else{if(s&&!s.standalone)return!1;o=e}let a=r.has(o);if(s){if(a)return!1;if(r.add(o),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)oi(l,t,n,r)}}else if(i){if(i.imports!=null&&!a){r.add(o);let l;pi(i.imports,d=>{oi(d,t,n,r)&&(l||=[],l.push(d))}),l!==void 0&&ku(l,t)}if(!a){let l=tn(o)||(()=>new o);t({provide:o,useFactory:l,deps:me},o),t({provide:Qa,useValue:o,multi:!0},o),t({provide:Or,useValue:()=>M(o),multi:!0},o)}let c=i.providers;if(c!=null&&!a){let l=e;Xa(c,d=>{t(d,l)})}}else return!1;return o!==e&&e.providers!==void 0}function Xa(e,t){for(let n of e)ja(n)&&(n=n.\u0275providers),Array.isArray(n)?Xa(n,t):t(n)}var Tv=V({provide:String,useValue:V});function Pu(e){return e!==null&&typeof e=="object"&&Tv in e}function Sv(e){return!!(e&&e.useExisting)}function Av(e){return!!(e&&e.useFactory)}function nn(e){return typeof e=="function"}function Lu(e){return!!e.useClass}var kr=new g(""),ni={},Cu={},Ia;function Ln(){return Ia===void 0&&(Ia=new Tr),Ia}var le=class{},rn=class extends le{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(t,n,r,o){super(),this.parent=n,this.source=r,this.scopes=o,Na(t,s=>this.processProvider(s)),this.records.set(Ya,kn(void 0,this)),o.has("environment")&&this.records.set(le,kn(void 0,this));let i=this.records.get(kr);i!=null&&typeof i.value=="string"&&this.scopes.add(i.value),this.injectorDefTypes=new Set(this.get(Qa,me,{self:!0}))}retrieve(t,n){let r=en(n)||0;try{return this.get(t,Jt,r)}catch(o){if(On(o))return o;throw o}}destroy(){xr(this),this._destroyed=!0;let t=w(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let n=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of n)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),w(t)}}onDestroy(t){return xr(this),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){xr(this);let n=Ye(this),r=Ce(void 0),o;try{return t()}finally{Ye(n),Ce(r)}}get(t,n=Jt,r){if(xr(this),t.hasOwnProperty(Eu))return t[Eu](this);let o=en(r),i,s=Ye(this),a=Ce(void 0);try{if(!(o&4)){let l=this.records.get(t);if(l===void 0){let d=kv(t)&&di(t);d&&this.injectableDefInScope(d)?l=kn(Aa(t),ni):l=null,this.records.set(t,l)}if(l!=null)return this.hydrate(t,l,o)}let c=o&2?Ln():this.parent;return n=o&8&&n===Jt?null:n,c.get(t,n)}catch(c){let l=Dv(c);throw l===-200||l===-201?new _(l,null):c}finally{Ce(a),Ye(s)}}resolveInjectorInitializers(){let t=w(null),n=Ye(this),r=Ce(void 0),o;try{let i=this.get(Or,me,{self:!0});for(let s of i)s()}finally{Ye(n),Ce(r),w(t)}}toString(){return"R3Injector[...]"}processProvider(t){t=ae(t);let n=nn(t)?t:ae(t&&t.provide),r=Rv(t);if(!nn(t)&&t.multi===!0){let o=this.records.get(n);o||(o=kn(void 0,ni,!0),o.factory=()=>Sa(o.multi),this.records.set(n,o)),n=t,o.multi.push(t)}this.records.set(n,r)}hydrate(t,n,r){let o=w(null);try{if(n.value===Cu)throw Wa("");return n.value===ni&&(n.value=Cu,n.value=n.factory(void 0,r)),typeof n.value=="object"&&n.value&&Ov(n.value)&&this._ngOnDestroyHooks.add(n.value),n.value}finally{w(o)}}injectableDefInScope(t){if(!t.providedIn)return!1;let n=ae(t.providedIn);return typeof n=="string"?n==="any"||this.scopes.has(n):this.injectorDefTypes.has(n)}removeOnDestroy(t){let n=this._onDestroyHooks.indexOf(t);n!==-1&&this._onDestroyHooks.splice(n,1)}};function Aa(e){let t=di(e),n=t!==null?t.factory:tn(e);if(n!==null)return n;if(e instanceof g)throw new _(-204,!1);if(e instanceof Function)return Nv(e);throw new _(-204,!1)}function Nv(e){if(e.length>0)throw new _(-204,!1);let n=yv(e);return n!==null?()=>n.factory(e):()=>new e}function Rv(e){if(Pu(e))return kn(void 0,e.useValue);{let t=Ja(e);return kn(t,ni)}}function Ja(e,t,n){let r;if(nn(e)){let o=ae(e);return tn(o)||Aa(o)}else if(Pu(e))r=()=>ae(e.useValue);else if(Av(e))r=()=>e.useFactory(...Sa(e.deps||[]));else if(Sv(e))r=(o,i)=>M(ae(e.useExisting),i!==void 0&&i&8?8:void 0);else{let o=ae(e&&(e.useClass||e.provide));if(Fv(e))r=()=>new o(...Sa(e.deps));else return tn(o)||Aa(o)}return r}function xr(e){if(e.destroyed)throw new _(-205,!1)}function kn(e,t,n=!1){return{factory:e,value:t,multi:n?[]:void 0}}function Fv(e){return!!e.deps}function Ov(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function kv(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function Na(e,t){for(let n of e)Array.isArray(n)?Na(n,t):n&&ja(n)?Na(n.\u0275providers,t):t(n)}function Vn(e,t){let n;e instanceof rn?(xr(e),n=e):n=new Ta(e);let r,o=Ye(n),i=Ce(void 0);try{return t()}finally{Ye(o),Ce(i)}}function Vu(){return Au()!==void 0||ti()!=null}var Pe=0,D=1,E=2,re=3,Te=4,ye=5,jn=6,Bn=7,ue=8,At=9,Xe=10,q=11,Hn=12,ec=13,sn=14,Ee=15,Nt=16,an=17,Je=18,Rt=19,tc=20,pt=21,yi=22,It=23,we=24,cn=25,Un=26,Q=27,ju=1;var Ft=7,Pr=8,ln=9,he=10;function mt(e){return Array.isArray(e)&&typeof e[ju]=="object"}function Le(e){return Array.isArray(e)&&e[ju]===!0}function nc(e){return(e.flags&4)!==0}function ht(e){return e.componentOffset>-1}function Lr(e){return(e.flags&1)===1}function et(e){return!!e.template}function $n(e){return(e[E]&512)!==0}function dn(e){return(e[E]&256)===256}var Bu="svg",Hu="math";function Se(e){for(;Array.isArray(e);)e=e[Pe];return e}function rc(e,t){return Se(t[e])}function Ve(e,t){return Se(t[e.index])}function bi(e,t){return e.data[t]}function Uu(e,t){return e[t]}function Ae(e,t){let n=t[e];return mt(n)?n:n[Pe]}function $u(e){return(e[E]&4)===4}function _i(e){return(e[E]&128)===128}function zu(e){return Le(e[re])}function tt(e,t){return t==null?null:e[t]}function oc(e){e[an]=0}function ic(e){e[E]&1024||(e[E]|=1024,_i(e)&&un(e))}function Gu(e,t){for(;e>0;)t=t[sn],e--;return t}function Vr(e){return!!(e[E]&9216||e[we]?.dirty)}function Di(e){e[Xe].changeDetectionScheduler?.notify(8),e[E]&64&&(e[E]|=1024),Vr(e)&&un(e)}function un(e){e[Xe].changeDetectionScheduler?.notify(0);let t=xt(e);for(;t!==null&&!(t[E]&8192||(t[E]|=8192,!_i(t)));)t=xt(t)}function sc(e,t){if(dn(e))throw new _(911,!1);e[pt]===null&&(e[pt]=[]),e[pt].push(t)}function Wu(e,t){if(e[pt]===null)return;let n=e[pt].indexOf(t);n!==-1&&e[pt].splice(n,1)}function xt(e){let t=e[re];return Le(t)?t[re]:t}function ac(e){return e[Bn]??=[]}function cc(e){return e.cleanup??=[]}function qu(e,t,n,r){let o=ac(t);o.push(n),e.firstCreatePass&&cc(e).push(r,o.length-1)}var T={lFrame:of(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Ra=!1;function Zu(){return T.lFrame.elementDepthCount}function Yu(){T.lFrame.elementDepthCount++}function lc(){T.lFrame.elementDepthCount--}function dc(){return T.bindingsEnabled}function uc(){return T.skipHydrationRootTNode!==null}function fc(e){return T.skipHydrationRootTNode===e}function pc(){T.skipHydrationRootTNode=null}function S(){return T.lFrame.lView}function ee(){return T.lFrame.tView}function Ei(e){return T.lFrame.contextLView=e,e[ue]}function Ci(e){return T.lFrame.contextLView=null,e}function be(){let e=mc();for(;e!==null&&e.type===64;)e=e.parent;return e}function mc(){return T.lFrame.currentTNode}function Qu(){let e=T.lFrame,t=e.currentTNode;return e.isParent?t:t.parent}function zn(e,t){let n=T.lFrame;n.currentTNode=e,n.isParent=t}function hc(){return T.lFrame.isParent}function gc(){T.lFrame.isParent=!1}function Ku(){return T.lFrame.contextLView}function vc(){return Ra}function Sr(e){let t=Ra;return Ra=e,t}function Xu(e){return T.lFrame.bindingIndex=e}function Gn(){return T.lFrame.bindingIndex++}function yc(e){let t=T.lFrame,n=t.bindingIndex;return t.bindingIndex=t.bindingIndex+e,n}function Ju(){return T.lFrame.inI18n}function ef(e,t){let n=T.lFrame;n.bindingIndex=n.bindingRootIndex=e,wi(t)}function tf(){return T.lFrame.currentDirectiveIndex}function wi(e){T.lFrame.currentDirectiveIndex=e}function nf(e){let t=T.lFrame.currentDirectiveIndex;return t===-1?null:e[t]}function Ii(){return T.lFrame.currentQueryIndex}function jr(e){T.lFrame.currentQueryIndex=e}function Pv(e){let t=e[D];return t.type===2?t.declTNode:t.type===1?e[ye]:null}function bc(e,t,n){if(n&4){let o=t,i=e;for(;o=o.parent,o===null&&!(n&1);)if(o=Pv(i),o===null||(i=i[sn],o.type&10))break;if(o===null)return!1;t=o,e=i}let r=T.lFrame=rf();return r.currentTNode=t,r.lView=e,!0}function xi(e){let t=rf(),n=e[D];T.lFrame=t,t.currentTNode=n.firstChild,t.lView=e,t.tView=n,t.contextLView=e,t.bindingIndex=n.bindingStartIndex,t.inI18n=!1}function rf(){let e=T.lFrame,t=e===null?null:e.child;return t===null?of(e):t}function of(e){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=t),t}function sf(){let e=T.lFrame;return T.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var _c=sf;function Mi(){let e=sf();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function af(e){return(T.lFrame.contextLView=Gu(e,T.lFrame.contextLView))[ue]}function Ot(){return T.lFrame.selectedIndex}function kt(e){T.lFrame.selectedIndex=e}function Ti(){let e=T.lFrame;return bi(e.tView,e.selectedIndex)}function cf(){return T.lFrame.currentNamespace}var lf=!0;function Si(){return lf}function Ai(e){lf=e}function Fa(e,t=null,n=null,r){let o=df(e,t,n,r);return o.resolveInjectorInitializers(),o}function df(e,t=null,n=null,r,o=new Set){let i=[n||me,vi(e)],s;return new rn(i,t||Ln(),s||null,o)}var de=class e{static THROW_IF_NOT_FOUND=Jt;static NULL=new Tr;static create(t,n){if(Array.isArray(t))return Fa({name:""},n,t,"");{let r=t.name??"";return Fa({name:r},t.parent,t.providers,r)}}static \u0275prov=v({token:e,providedIn:"any",factory:()=>M(Ya)});static __NG_ELEMENT_ID__=-1},Z=new g(""),Pt=(()=>{class e{static __NG_ELEMENT_ID__=Lv;static __NG_ENV_ID__=n=>n}return e})(),ii=class extends Pt{_lView;constructor(t){super(),this._lView=t}get destroyed(){return dn(this._lView)}onDestroy(t){let n=this._lView;return sc(n,t),()=>Wu(n,t)}};function Lv(){return new ii(S())}var uf=!1,ff=new g(""),fn=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Qt(!1);debugTaskTracker=m(ff,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new F(n=>{n.next(!1),n.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let n=this.taskId++;return this.pendingTasks.add(n),this.debugTaskTracker?.add(n),n}has(n){return this.pendingTasks.has(n)}remove(n){this.pendingTasks.delete(n),this.debugTaskTracker?.remove(n),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),Oa=class extends G{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(t=!1){super(),this.__isAsync=t,Vu()&&(this.destroyRef=m(Pt,{optional:!0})??void 0,this.pendingTasks=m(fn,{optional:!0})??void 0)}emit(t){let n=w(null);try{super.next(t)}finally{w(n)}}subscribe(t,n,r){let o=t,i=n||(()=>null),s=r;if(t&&typeof t=="object"){let c=t;o=c.next?.bind(c),i=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(i=this.wrapInTimeout(i),o&&(o=this.wrapInTimeout(o)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:o,error:i,complete:s});return t instanceof se&&t.add(a),a}wrapInTimeout(t){return n=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{t(n)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},ce=Oa;function si(...e){}function Dc(e){let t,n;function r(){e=si;try{n!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(n),t!==void 0&&clearTimeout(t)}catch{}}return t=setTimeout(()=>{e(),r()}),typeof requestAnimationFrame=="function"&&(n=requestAnimationFrame(()=>{e(),r()})),()=>r()}function pf(e){return queueMicrotask(()=>e()),()=>{e=si}}var Ec="isAngularZone",Ar=Ec+"_ID",Vv=0,N=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ce(!1);onMicrotaskEmpty=new ce(!1);onStable=new ce(!1);onError=new ce(!1);constructor(t){let{enableLongStackTrace:n=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:o=!1,scheduleInRootZone:i=uf}=t;if(typeof Zone>"u")throw new _(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),n&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!o&&r,s.shouldCoalesceRunChangeDetection=o,s.callbackScheduled=!1,s.scheduleInRootZone=i,Hv(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Ec)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new _(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new _(909,!1)}run(t,n,r){return this._inner.run(t,n,r)}runTask(t,n,r,o){let i=this._inner,s=i.scheduleEventTask("NgZoneEvent: "+o,t,jv,si,si);try{return i.runTask(s,n,r)}finally{i.cancelTask(s)}}runGuarded(t,n,r){return this._inner.runGuarded(t,n,r)}runOutsideAngular(t){return this._outer.run(t)}},jv={};function Cc(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function Bv(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function t(){Dc(()=>{e.callbackScheduled=!1,ka(e),e.isCheckStableRunning=!0,Cc(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{t()}):e._outer.run(()=>{t()}),ka(e)}function Hv(e){let t=()=>{Bv(e)},n=Vv++;e._inner=e._inner.fork({name:"angular",properties:{[Ec]:!0,[Ar]:n,[Ar+n]:!0},onInvokeTask:(r,o,i,s,a,c)=>{if(Uv(c))return r.invokeTask(i,s,a,c);try{return wu(e),r.invokeTask(i,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&t(),Iu(e)}},onInvoke:(r,o,i,s,a,c,l)=>{try{return wu(e),r.invoke(i,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!$v(c)&&t(),Iu(e)}},onHasTask:(r,o,i,s)=>{r.hasTask(i,s),o===i&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,ka(e),Cc(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,o,i,s)=>(r.handleError(i,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function ka(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function wu(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function Iu(e){e._nesting--,Cc(e)}var Nr=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ce;onMicrotaskEmpty=new ce;onStable=new ce;onError=new ce;run(t,n,r){return t.apply(n,r)}runGuarded(t,n,r){return t.apply(n,r)}runOutsideAngular(t){return t()}runTask(t,n,r,o){return t.apply(n,r)}};function Uv(e){return mf(e,"__ignore_ng_zone__")}function $v(e){return mf(e,"__scheduler_tick__")}function mf(e,t){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[t]===!0}var Qe=class{_console=console;handleError(t){this._console.error("ERROR",t)}},Lt=new g("",{factory:()=>{let e=m(N),t=m(le),n;return r=>{e.runOutsideAngular(()=>{t.destroyed&&!n?setTimeout(()=>{throw r}):(n??=t.get(Qe),n.handleError(r))})}}}),hf={provide:Or,useValue:()=>{let e=m(Qe,{optional:!0})},multi:!0};function Ne(e,t){let[n,r,o]=ta(e,t?.equal),i=n,s=i[ie];return i.set=r,i.update=o,i.asReadonly=gf.bind(i),i}function gf(){let e=this[ie];if(e.readonlyFn===void 0){let t=()=>this();t[ie]=e,e.readonlyFn=t}return e.readonlyFn}var Br=(()=>{class e{view;node;constructor(n,r){this.view=n,this.node=r}static __NG_ELEMENT_ID__=zv}return e})();function zv(){return new Br(S(),be())}var Ke=class{},Hr=new g("",{factory:()=>!0});var wc=new g(""),Ur=(()=>{class e{internalPendingTasks=m(fn);scheduler=m(Ke);errorHandler=m(Lt);add(){let n=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(n)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(n))}}run(n){let r=this.add();n().catch(this.errorHandler).finally(r)}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),Ni=(()=>{class e{static \u0275prov=v({token:e,providedIn:"root",factory:()=>new Pa})}return e})(),Pa=class{dirtyEffectCount=0;queues=new Map;add(t){this.enqueue(t),this.schedule(t)}schedule(t){t.dirty&&this.dirtyEffectCount++}remove(t){let n=t.zone,r=this.queues.get(n);r.has(t)&&(r.delete(t),t.dirty&&this.dirtyEffectCount--)}enqueue(t){let n=t.zone;this.queues.has(n)||this.queues.set(n,new Set);let r=this.queues.get(n);r.has(t)||r.add(t)}flush(){for(;this.dirtyEffectCount>0;){let t=!1;for(let[n,r]of this.queues)n===null?t||=this.flushQueue(r):t||=n.run(()=>this.flushQueue(r));t||(this.dirtyEffectCount=0)}}flushQueue(t){let n=!1;for(let r of t)r.dirty&&(this.dirtyEffectCount--,n=!0,r.run());return n}},ai=class{[ie];constructor(t){this[ie]=t}destroy(){this[ie].destroy()}};function Ic(e,t){let n=t?.injector??m(de),r=t?.manualCleanup!==!0?n.get(Pt):null,o,i=n.get(Br,null,{optional:!0}),s=n.get(Ke);return i!==null?(o=qv(i.view,s,e),r instanceof ii&&r._lView===i.view&&(r=null)):o=Zv(e,n.get(Ni),s),o.injector=n,r!==null&&(o.onDestroyFns=[r.onDestroy(()=>o.destroy())]),new ai(o)}var vf=P(I({},ra),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=Sr(!1);try{oa(this)}finally{Sr(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=w(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],w(e)}}}),Gv=P(I({},vf),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Et(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),Wv=P(I({},vf),{consumerMarkedDirty(){this.view[E]|=8192,un(this.view),this.notifier.notify(13)},destroy(){if(Et(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[It]?.delete(this)}});function qv(e,t,n){let r=Object.create(Wv);return r.view=e,r.zone=typeof Zone<"u"?Zone.current:null,r.notifier=t,r.fn=yf(r,n),e[It]??=new Set,e[It].add(r),r.consumerMarkedDirty(r),r}function Zv(e,t,n){let r=Object.create(Gv);return r.fn=yf(r,e),r.scheduler=t,r.notifier=n,r.zone=typeof Zone<"u"?Zone.current:null,r.scheduler.add(r),r.notifier.notify(12),r}function yf(e,t){return()=>{t(n=>(e.cleanupFns??=[]).push(n))}}function Qr(e){return{toString:e}.toString()}function iy(e){return typeof e=="function"}function Wf(e,t,n,r){t!==null?t.applyValueToInputSignal(t,r):e[n]=r}var Li=class{previousValue;currentValue;firstChange;constructor(t,n,r){this.previousValue=t,this.currentValue=n,this.firstChange=r}isFirstChange(){return this.firstChange}},vn=(()=>{let e=()=>qf;return e.ngInherit=!0,e})();function qf(e){return e.type.prototype.ngOnChanges&&(e.setInput=ay),sy}function sy(){let e=Yf(this),t=e?.current;if(t){let n=e.previous;if(n===St)e.previous=t;else for(let r in t)n[r]=t[r];e.current=null,this.ngOnChanges(t)}}function ay(e,t,n,r,o){let i=this.declaredInputs[r],s=Yf(e)||cy(e,{previous:St,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[i];a[i]=new Li(l&&l.currentValue,n,c===St),Wf(e,t,o,n)}var Zf="__ngSimpleChanges__";function Yf(e){return e[Zf]||null}function cy(e,t){return e[Zf]=t}var bf=[];var R=function(e,t=null,n){for(let r=0;r<bf.length;r++){let o=bf[r];o(e,t,n)}},A=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(A||{});function ly(e,t,n){let{ngOnChanges:r,ngOnInit:o,ngDoCheck:i}=t.type.prototype;if(r){let s=qf(t);(n.preOrderHooks??=[]).push(e,s),(n.preOrderCheckHooks??=[]).push(e,s)}o&&(n.preOrderHooks??=[]).push(0-e,o),i&&((n.preOrderHooks??=[]).push(e,i),(n.preOrderCheckHooks??=[]).push(e,i))}function Qf(e,t){for(let n=t.directiveStart,r=t.directiveEnd;n<r;n++){let i=e.data[n].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=i;s&&(e.contentHooks??=[]).push(-n,s),a&&((e.contentHooks??=[]).push(n,a),(e.contentCheckHooks??=[]).push(n,a)),c&&(e.viewHooks??=[]).push(-n,c),l&&((e.viewHooks??=[]).push(n,l),(e.viewCheckHooks??=[]).push(n,l)),d!=null&&(e.destroyHooks??=[]).push(n,d)}}function Fi(e,t,n){Kf(e,t,3,n)}function Oi(e,t,n,r){(e[E]&3)===n&&Kf(e,t,n,r)}function xc(e,t){let n=e[E];(n&3)===t&&(n&=16383,n+=1,e[E]=n)}function Kf(e,t,n,r){let o=r!==void 0?e[an]&65535:0,i=r??-1,s=t.length-1,a=0;for(let c=o;c<s;c++)if(typeof t[c+1]=="number"){if(a=t[c],r!=null&&a>=r)break}else t[c]<0&&(e[an]+=65536),(a<i||i==-1)&&(dy(e,n,t,c),e[an]=(e[an]&4294901760)+c+2),c++}function _f(e,t){R(A.LifecycleHookStart,e,t);let n=w(null);try{t.call(e)}finally{w(n),R(A.LifecycleHookEnd,e,t)}}function dy(e,t,n,r){let o=n[r]<0,i=n[r+1],s=o?-n[r]:n[r],a=e[s];o?e[E]>>14<e[an]>>16&&(e[E]&3)===t&&(e[E]+=16384,_f(a,i)):_f(a,i)}var qn=-1,mn=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(t,n,r,o){this.factory=t,this.name=o,this.canSeeViewProviders=n,this.injectImpl=r}};function uy(e){return(e.flags&8)!==0}function fy(e){return(e.flags&16)!==0}function py(e,t,n){let r=0;for(;r<n.length;){let o=n[r];if(typeof o=="number"){if(o!==0)break;r++;let i=n[r++],s=n[r++],a=n[r++];e.setAttribute(t,s,a,i)}else{let i=o,s=n[++r];hy(i)?e.setProperty(t,i,s):e.setAttribute(t,i,s),r++}}return r}function my(e){return e===3||e===4||e===6}function hy(e){return e.charCodeAt(0)===64}function Zn(e,t){if(!(t===null||t.length===0))if(e===null||e.length===0)e=t.slice();else{let n=-1;for(let r=0;r<t.length;r++){let o=t[r];typeof o=="number"?n=o:n===0||(n===-1||n===2?Df(e,n,o,null,t[++r]):Df(e,n,o,null,null))}}return e}function Df(e,t,n,r,o){let i=0,s=e.length;if(t===-1)s=-1;else for(;i<e.length;){let a=e[i++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=i-1;break}}}for(;i<e.length;){let a=e[i];if(typeof a=="number")break;if(a===n){o!==null&&(e[i+1]=o);return}i++,o!==null&&i++}s!==-1&&(e.splice(s,0,t),i=s+1),e.splice(i++,0,n),o!==null&&e.splice(i++,0,o)}function Xf(e){return e!==qn}function Vi(e){return e&32767}function gy(e){return e>>16}function ji(e,t){let n=gy(e),r=t;for(;n>0;)r=r[sn],n--;return r}var Pc=!0;function Ef(e){let t=Pc;return Pc=e,t}var vy=256,Jf=vy-1,ep=5,yy=0,nt={};function by(e,t,n){let r;typeof n=="string"?r=n.charCodeAt(0)||0:n.hasOwnProperty(on)&&(r=n[on]),r==null&&(r=n[on]=yy++);let o=r&Jf,i=1<<o;t.data[e+(o>>ep)]|=i}function Bi(e,t){let n=tp(e,t);if(n!==-1)return n;let r=t[D];r.firstCreatePass&&(e.injectorIndex=t.length,Mc(r.data,e),Mc(t,null),Mc(r.blueprint,null));let o=cl(e,t),i=e.injectorIndex;if(Xf(o)){let s=Vi(o),a=ji(o,t),c=a[D].data;for(let l=0;l<8;l++)t[i+l]=a[s+l]|c[s+l]}return t[i+8]=o,i}function Mc(e,t){e.push(0,0,0,0,0,0,0,0,t)}function tp(e,t){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||t[e.injectorIndex+8]===null?-1:e.injectorIndex}function cl(e,t){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let n=0,r=null,o=t;for(;o!==null;){if(r=sp(o),r===null)return qn;if(n++,o=o[sn],r.injectorIndex!==-1)return r.injectorIndex|n<<16}return qn}function Lc(e,t,n){by(e,t,n)}function np(e,t,n){if(n&8||e!==void 0)return e;fi(t,"NodeInjector")}function rp(e,t,n,r){if(n&8&&r===void 0&&(r=null),(n&3)===0){let o=e[At],i=Ce(void 0);try{return o?o.get(t,r,n&8):qa(t,r,n&8)}finally{Ce(i)}}return np(r,t,n)}function op(e,t,n,r=0,o){if(e!==null){if(t[E]&2048&&!(r&2)){let s=Cy(e,t,n,r,nt);if(s!==nt)return s}let i=ip(e,t,n,r,nt);if(i!==nt)return i}return rp(t,n,r,o)}function ip(e,t,n,r,o){let i=Dy(n);if(typeof i=="function"){if(!bc(t,e,r))return r&1?np(o,n,r):rp(t,n,r,o);try{let s;if(s=i(r),s==null&&!(r&8))fi(n);else return s}finally{_c()}}else if(typeof i=="number"){let s=null,a=tp(e,t),c=qn,l=r&1?t[Ee][ye]:null;for((a===-1||r&4)&&(c=a===-1?cl(e,t):t[a+8],c===qn||!wf(r,!1)?a=-1:(s=t[D],a=Vi(c),t=ji(c,t)));a!==-1;){let d=t[D];if(Cf(i,a,d.data)){let u=_y(a,t,n,s,r,l);if(u!==nt)return u}c=t[a+8],c!==qn&&wf(r,t[D].data[a+8]===l)&&Cf(i,a,t)?(s=d,a=Vi(c),t=ji(c,t)):a=-1}}return o}function _y(e,t,n,r,o,i){let s=t[D],a=s.data[e+8],c=r==null?ht(a)&&Pc:r!=s&&(a.type&3)!==0,l=o&1&&i===a,d=ki(a,s,n,c,l);return d!==null?Gr(t,s,d,a,o):nt}function ki(e,t,n,r,o){let i=e.providerIndexes,s=t.data,a=i&1048575,c=e.directiveStart,l=e.directiveEnd,d=i>>20,u=r?a:a+d,p=o?a+d:l;for(let f=u;f<p;f++){let h=s[f];if(f<c&&n===h||f>=c&&h.type===n)return f}if(o){let f=s[c];if(f&&et(f)&&f.type===n)return c}return null}function Gr(e,t,n,r,o){let i=e[n],s=t.data;if(i instanceof mn){let a=i;if(a.resolving)throw Wa("");let c=Ef(a.canSeeViewProviders);a.resolving=!0;let l=s[n].type||s[n],d,u=a.injectImpl?Ce(a.injectImpl):null,p=bc(e,r,0);try{i=e[n]=a.factory(void 0,o,s,e,r),t.firstCreatePass&&n>=r.directiveStart&&ly(n,s[n],t)}finally{u!==null&&Ce(u),Ef(c),a.resolving=!1,_c()}}return i}function Dy(e){if(typeof e=="string")return e.charCodeAt(0)||0;let t=e.hasOwnProperty(on)?e[on]:void 0;return typeof t=="number"?t>=0?t&Jf:Ey:t}function Cf(e,t,n){let r=1<<e;return!!(n[t+(e>>ep)]&r)}function wf(e,t){return!(e&2)&&!(e&1&&t)}var pn=class{_tNode;_lView;constructor(t,n){this._tNode=t,this._lView=n}get(t,n,r){return op(this._tNode,this._lView,t,en(r),n)}};function Ey(){return new pn(be(),S())}function Qi(e){return Qr(()=>{let t=e.prototype.constructor,n=t[Mr]||Vc(t),r=Object.prototype,o=Object.getPrototypeOf(e.prototype).constructor;for(;o&&o!==r;){let i=o[Mr]||Vc(o);if(i&&i!==n)return i;o=Object.getPrototypeOf(o)}return i=>new i})}function Vc(e){return Va(e)?()=>{let t=Vc(ae(e));return t&&t()}:tn(e)}function Cy(e,t,n,r,o){let i=e,s=t;for(;i!==null&&s!==null&&s[E]&2048&&!$n(s);){let a=ip(i,s,n,r|2,nt);if(a!==nt)return a;let c=i.parent;if(!c){let l=s[tc];if(l){let d=l.get(n,nt,r&-5);if(d!==nt)return d}c=sp(s),s=s[sn]}i=c}return o}function sp(e){let t=e[D],n=t.type;return n===2?t.declTNode:n===1?e[ye]:null}function wy(){return Jn(be(),S())}function Jn(e,t){return new K(Ve(e,t))}var K=(()=>{class e{nativeElement;constructor(n){this.nativeElement=n}static __NG_ELEMENT_ID__=wy}return e})();function ap(e){return e instanceof K?e.nativeElement:e}function Iy(){return this._results[Symbol.iterator]()}var Hi=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new G}constructor(t=!1){this._emitDistinctChangesOnly=t}get(t){return this._results[t]}map(t){return this._results.map(t)}filter(t){return this._results.filter(t)}find(t){return this._results.find(t)}reduce(t,n){return this._results.reduce(t,n)}forEach(t){this._results.forEach(t)}some(t){return this._results.some(t)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(t,n){this.dirty=!1;let r=Ru(t);(this._changesDetected=!Nu(this._results,r,n))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(t){this._onDirty=t}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Iy};function cp(e){return(e.flags&128)===128}var ll=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(ll||{}),lp=new Map,xy=0;function My(){return xy++}function Ty(e){lp.set(e[Rt],e)}function jc(e){lp.delete(e[Rt])}var If="__ngContext__";function Yn(e,t){mt(t)?(e[If]=t[Rt],Ty(t)):e[If]=t}function dp(e){return fp(e[Hn])}function up(e){return fp(e[Te])}function fp(e){for(;e!==null&&!Le(e);)e=e[Te];return e}var Sy;function dl(e){Sy=e}var er=new g("",{factory:()=>Ay}),Ay="ng";var Ki=new g(""),yn=new g("",{providedIn:"platform",factory:()=>"unknown"}),ul=new g(""),tr=new g("",{factory:()=>m(Z).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var pp=!1,mp=new g("",{factory:()=>pp});var Ny=(e,t,n,r)=>{};function Ry(e,t,n,r){Ny(e,t,n,r)}function Xi(e){return(e.flags&32)===32}var Fy=()=>null;function hp(e,t,n=!1){return Fy(e,t,n)}function gp(e,t){let n=e.contentQueries;if(n!==null){let r=w(null);try{for(let o=0;o<n.length;o+=2){let i=n[o],s=n[o+1];if(s!==-1){let a=e.data[s];jr(i),a.contentQueries(2,t[s],s)}}}finally{w(r)}}}function Bc(e,t,n){jr(0);let r=w(null);try{t(e,n)}finally{w(r)}}function vp(e,t,n){if(nc(t)){let r=w(null);try{let o=t.directiveStart,i=t.directiveEnd;for(let s=o;s<i;s++){let a=e.data[s];if(a.contentQueries){let c=n[s];a.contentQueries(1,c,s)}}}finally{w(r)}}}var He=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(He||{});var Hc=class{changingThisBreaksApplicationSecurity;constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${La})`}};function fl(e){return e instanceof Hc?e.changingThisBreaksApplicationSecurity:e}function Oy(e,t){return e.createText(t)}function ky(e,t,n){e.setValue(t,n)}function yp(e,t,n){return e.createElement(t,n)}function Ui(e,t,n,r,o){e.insertBefore(t,n,r,o)}function bp(e,t,n){e.appendChild(t,n)}function xf(e,t,n,r,o){r!==null?Ui(e,t,n,r,o):bp(e,t,n)}function Py(e,t,n,r){e.removeChild(null,t,n,r)}function Ly(e,t,n){e.setAttribute(t,"style",n)}function Vy(e,t,n){n===""?e.removeAttribute(t,"class"):e.setAttribute(t,"class",n)}function _p(e,t,n){let{mergedAttrs:r,classes:o,styles:i}=n;r!==null&&py(e,t,r),o!==null&&Vy(e,t,o),i!==null&&Ly(e,t,i)}function jy(e,t,n){let r=e.length;for(;;){let o=e.indexOf(t,n);if(o===-1)return o;if(o===0||e.charCodeAt(o-1)<=32){let i=t.length;if(o+i===r||e.charCodeAt(o+i)<=32)return o}n=o+1}}var Dp="ng-template";function By(e,t,n,r){let o=0;if(r){for(;o<t.length&&typeof t[o]=="string";o+=2)if(t[o]==="class"&&jy(t[o+1].toLowerCase(),n,0)!==-1)return!0}else if(pl(e))return!1;if(o=t.indexOf(1,o),o>-1){let i;for(;++o<t.length&&typeof(i=t[o])=="string";)if(i.toLowerCase()===n)return!0}return!1}function pl(e){return e.type===4&&e.value!==Dp}function Hy(e,t,n){let r=e.type===4&&!n?Dp:e.value;return t===r}function Uy(e,t,n){let r=4,o=e.attrs,i=o!==null?Gy(o):0,s=!1;for(let a=0;a<t.length;a++){let c=t[a];if(typeof c=="number"){if(!s&&!je(r)&&!je(c))return!1;if(s&&je(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!Hy(e,c,n)||c===""&&t.length===1){if(je(r))return!1;s=!0}}else if(r&8){if(o===null||!By(e,o,c,n)){if(je(r))return!1;s=!0}}else{let l=t[++a],d=$y(c,o,pl(e),n);if(d===-1){if(je(r))return!1;s=!0;continue}if(l!==""){let u;if(d>i?u="":u=o[d+1].toLowerCase(),r&2&&l!==u){if(je(r))return!1;s=!0}}}}return je(r)||s}function je(e){return(e&1)===0}function $y(e,t,n,r){if(t===null)return-1;let o=0;if(r||!n){let i=!1;for(;o<t.length;){let s=t[o];if(s===e)return o;if(s===3||s===6)i=!0;else if(s===1||s===2){let a=t[++o];for(;typeof a=="string";)a=t[++o];continue}else{if(s===4)break;if(s===0){o+=4;continue}}o+=i?1:2}return-1}else return Wy(t,e)}function Ep(e,t,n=!1){for(let r=0;r<t.length;r++)if(Uy(e,t[r],n))return!0;return!1}function zy(e){let t=e.attrs;if(t!=null){let n=t.indexOf(5);if((n&1)===0)return t[n+1]}return null}function Gy(e){for(let t=0;t<e.length;t++){let n=e[t];if(my(n))return t}return e.length}function Wy(e,t){let n=e.indexOf(4);if(n>-1)for(n++;n<e.length;){let r=e[n];if(typeof r=="number")return-1;if(r===t)return n;n++}return-1}function qy(e,t){e:for(let n=0;n<t.length;n++){let r=t[n];if(e.length===r.length){for(let o=0;o<e.length;o++)if(e[o]!==r[o])continue e;return!0}}return!1}function Mf(e,t){return e?":not("+t.trim()+")":t}function Zy(e){let t=e[0],n=1,r=2,o="",i=!1;for(;n<e.length;){let s=e[n];if(typeof s=="string")if(r&2){let a=e[++n];o+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?o+="."+s:r&4&&(o+=" "+s);else o!==""&&!je(s)&&(t+=Mf(i,o),o=""),r=s,i=i||!je(r);n++}return o!==""&&(t+=Mf(i,o)),t}function Yy(e){return e.map(Zy).join(",")}function Qy(e){let t=[],n=[],r=1,o=2;for(;r<e.length;){let i=e[r];if(typeof i=="string")o===2?i!==""&&t.push(i,e[++r]):o===8&&n.push(i);else{if(!je(o))break;o=i}r++}return n.length&&t.push(1,...n),t}var $e={};function ml(e,t,n,r,o,i,s,a,c,l,d){let u=Q+r,p=u+o,f=Ky(u,p),h=typeof l=="function"?l():l;return f[D]={type:e,blueprint:f,template:n,queries:null,viewQuery:a,declTNode:t,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof i=="function"?i():i,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:h,incompleteFirstPass:!1,ssrId:d}}function Ky(e,t){let n=[];for(let r=0;r<t;r++)n.push(r<e?null:$e);return n}function Xy(e){let t=e.tView;return t===null||t.incompleteFirstPass?e.tView=ml(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):t}function hl(e,t,n,r,o,i,s,a,c,l,d){let u=t.blueprint.slice();return u[Pe]=o,u[E]=r|4|128|8|64|1024,(l!==null||e&&e[E]&2048)&&(u[E]|=2048),oc(u),u[re]=u[sn]=e,u[ue]=n,u[Xe]=s||e&&e[Xe],u[q]=a||e&&e[q],u[At]=c||e&&e[At]||null,u[ye]=i,u[Rt]=My(),u[jn]=d,u[tc]=l,u[Ee]=t.type==2?e[Ee]:u,u}function Jy(e,t,n){let r=Ve(t,e),o=Xy(n),i=e[Xe].rendererFactory,s=gl(e,hl(e,o,null,Cp(n),r,t,null,i.createRenderer(r,n),null,null,null));return e[t.index]=s}function Cp(e){let t=16;return e.signals?t=4096:e.onPush&&(t=64),t}function wp(e,t,n,r){if(n===0)return-1;let o=t.length;for(let i=0;i<n;i++)t.push(r),e.blueprint.push(r),e.data.push(null);return o}function gl(e,t){return e[Hn]?e[ec][Te]=t:e[Hn]=t,e[ec]=t,t}function Y(e=1){Ip(ee(),S(),Ot()+e,!1)}function Ip(e,t,n,r){if(!r)if((t[E]&3)===3){let i=e.preOrderCheckHooks;i!==null&&Fi(t,i,n)}else{let i=e.preOrderHooks;i!==null&&Oi(t,i,0,n)}kt(n)}var Ji=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(Ji||{});function Uc(e,t,n,r){let o=w(null);try{let[i,s,a]=e.inputs[n],c=null;(s&Ji.SignalBased)!==0&&(c=t[i][ie]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):a!==null&&(r=a.call(t,r)),e.setInput!==null?e.setInput(t,c,r,n,i):Wf(t,c,i,r)}finally{w(o)}}var rt=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(rt||{}),eb;function vl(e,t){return eb(e,t)}var XS=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var $c=new WeakMap,zc=new WeakSet;function tb(e,t){let n=$c.get(e);if(!n||n.length===0)return;let r=t.parentNode,o=t.previousSibling;for(let i=n.length-1;i>=0;i--){let s=n[i],a=s.parentNode;s===t?(n.splice(i,1),zc.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(o&&s===o||a&&r&&a!==r)&&(n.splice(i,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function nb(e,t){let n=$c.get(e);n?n.includes(t)||n.push(t):$c.set(e,[t])}var Qn=new Set,es=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(es||{}),ot=new g(""),Tf=new Set;function Kr(e){Tf.has(e)||(Tf.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var yl=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),bl=[0,1,2,3],xp=(()=>{class e{ngZone=m(N);scheduler=m(Ke);errorHandler=m(Qe,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){m(ot,{optional:!0})}execute(){let n=this.sequences.size>0;n&&R(A.AfterRenderHooksStart),this.executing=!0;for(let r of bl)for(let o of this.sequences)if(!(o.erroredOrDestroyed||!o.hooks[r]))try{o.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let i=o.hooks[r];return i(o.pipelinedValue)},o.snapshot))}catch(i){o.erroredOrDestroyed=!0,this.errorHandler?.handleError(i)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),n&&R(A.AfterRenderHooksEnd)}register(n){let{view:r}=n;r!==void 0?((r[cn]??=[]).push(n),un(r),r[E]|=8192):this.executing?this.deferredRegistrations.add(n):this.addSequence(n)}addSequence(n){this.sequences.add(n),this.scheduler.notify(7)}unregister(n){this.executing&&this.sequences.has(n)?(n.erroredOrDestroyed=!0,n.pipelinedValue=void 0,n.once=!0):(this.sequences.delete(n),this.deferredRegistrations.delete(n))}maybeTrace(n,r){return r?r.run(es.AFTER_NEXT_RENDER,n):n()}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),$i=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(t,n,r,o,i,s=null){this.impl=t,this.hooks=n,this.view=r,this.once=o,this.snapshot=s,this.unregisterOnDestroy=i?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let t=this.view?.[cn];t&&(this.view[cn]=t.filter(n=>n!==this))}};var rb=new g("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:m(le)})});function Mp(e,t,n){let r=e.get(rb);if(Array.isArray(t))for(let o of t)r.queue.add(o),n?.detachedLeaveAnimationFns?.push(o);else r.queue.add(t),n?.detachedLeaveAnimationFns?.push(t);r.scheduler&&r.scheduler(e)}function ob(e,t){for(let[n,r]of t)Mp(e,r.animateFns)}function Sf(e,t,n,r){let o=e?.[Un]?.enter;t!==null&&o&&o.has(n.index)&&ob(r,o)}function Wn(e,t,n,r,o,i,s,a){if(o!=null){let c,l=!1;Le(o)?c=o:mt(o)&&(l=!0,o=o[Pe]);let d=Se(o);e===0&&r!==null?(Sf(a,r,i,n),s==null?bp(t,r,d):Ui(t,r,d,s||null,!0)):e===1&&r!==null?(Sf(a,r,i,n),Ui(t,r,d,s||null,!0),tb(i,d)):e===2?(a?.[Un]?.leave?.has(i.index)&&nb(i,d),Af(a,i,n,u=>{if(zc.has(d)){zc.delete(d);return}Py(t,d,l,u)})):e===3&&Af(a,i,n,()=>{t.destroyNode(d)}),c!=null&&hb(t,e,n,c,i,r,s)}}function ib(e,t){Tp(e,t),t[Pe]=null,t[ye]=null}function sb(e,t,n,r,o,i){r[Pe]=o,r[ye]=t,ts(e,r,n,1,o,i)}function Tp(e,t){t[Xe].changeDetectionScheduler?.notify(9),ts(e,t,t[q],2,null,null)}function ab(e){let t=e[Hn];if(!t)return Tc(e[D],e);for(;t;){let n=null;if(mt(t))n=t[Hn];else{let r=t[he];r&&(n=r)}if(!n){for(;t&&!t[Te]&&t!==e;)mt(t)&&Tc(t[D],t),t=t[re];t===null&&(t=e),mt(t)&&Tc(t[D],t),n=t&&t[Te]}t=n}}function _l(e,t){let n=e[ln],r=n.indexOf(t);n.splice(r,1)}function Dl(e,t){if(dn(t))return;let n=t[q];n.destroyNode&&ts(e,t,n,3,null,null),ab(t)}function Tc(e,t){if(dn(t))return;let n=w(null);try{t[E]&=-129,t[E]|=256,t[we]&&Et(t[we]),db(e,t),lb(e,t),t[D].type===1&&t[q].destroy();let r=t[Nt];if(r!==null&&Le(t[re])){r!==t[re]&&_l(r,t);let o=t[Je];o!==null&&o.detachView(e)}jc(t)}finally{w(n)}}function Af(e,t,n,r){let o=e?.[Un];if(o==null||o.leave==null||!o.leave.has(t.index))return r(!1);e&&Qn.add(e[Rt]),Mp(n,()=>{if(o.leave&&o.leave.has(t.index)){let s=o.leave.get(t.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}o.detachedLeaveAnimationFns=void 0}o.running=Promise.allSettled(a),cb(e,r)}else e&&Qn.delete(e[Rt]),r(!1)},o)}function cb(e,t){let n=e[Un]?.running;if(n){n.then(()=>{e[Un].running=void 0,Qn.delete(e[Rt]),t(!0)});return}t(!1)}function lb(e,t){let n=e.cleanup,r=t[Bn];if(n!==null)for(let s=0;s<n.length-1;s+=2)if(typeof n[s]=="string"){let a=n[s+3];a>=0?r[a]():r[-a].unsubscribe(),s+=2}else{let a=r[n[s+1]];n[s].call(a)}r!==null&&(t[Bn]=null);let o=t[pt];if(o!==null){t[pt]=null;for(let s=0;s<o.length;s++){let a=o[s];a()}}let i=t[It];if(i!==null){t[It]=null;for(let s of i)s.destroy()}}function db(e,t){let n;if(e!=null&&(n=e.destroyHooks)!=null)for(let r=0;r<n.length;r+=2){let o=t[n[r]];if(!(o instanceof mn)){let i=n[r+1];if(Array.isArray(i))for(let s=0;s<i.length;s+=2){let a=o[i[s]],c=i[s+1];R(A.LifecycleHookStart,a,c);try{c.call(a)}finally{R(A.LifecycleHookEnd,a,c)}}else{R(A.LifecycleHookStart,o,i);try{i.call(o)}finally{R(A.LifecycleHookEnd,o,i)}}}}}function Sp(e,t,n){return ub(e,t.parent,n)}function ub(e,t,n){let r=t;for(;r!==null&&r.type&168;)t=r,r=t.parent;if(r===null)return n[Pe];if(ht(r)){let{encapsulation:o}=e.data[r.directiveStart+r.componentOffset];if(o===He.None||o===He.Emulated)return null}return Ve(r,n)}function Ap(e,t,n){return pb(e,t,n)}function fb(e,t,n){return e.type&40?Ve(e,n):null}var pb=fb,Nf;function El(e,t,n,r){let o=Sp(e,r,t),i=t[q],s=r.parent||t[ye],a=Ap(s,r,t);if(o!=null)if(Array.isArray(n))for(let c=0;c<n.length;c++)xf(i,o,n[c],a,!1);else xf(i,o,n,a,!1);Nf!==void 0&&Nf(i,r,t,n,o)}function $r(e,t){if(t!==null){let n=t.type;if(n&3)return Ve(t,e);if(n&4)return Gc(-1,e[t.index]);if(n&8){let r=t.child;if(r!==null)return $r(e,r);{let o=e[t.index];return Le(o)?Gc(-1,o):Se(o)}}else{if(n&128)return $r(e,t.next);if(n&32)return vl(t,e)()||Se(e[t.index]);{let r=Np(e,t);if(r!==null){if(Array.isArray(r))return r[0];let o=xt(e[Ee]);return $r(o,r)}else return $r(e,t.next)}}}return null}function Np(e,t){if(t!==null){let r=e[Ee][ye],o=t.projection;return r.projection[o]}return null}function Gc(e,t){let n=he+e+1;if(n<t.length){let r=t[n],o=r[D].firstChild;if(o!==null)return $r(r,o)}return t[Ft]}function Cl(e,t,n,r,o,i,s){for(;n!=null;){let a=r[At];if(n.type===128){n=n.next;continue}let c=r[n.index],l=n.type;if(s&&t===0&&(c&&Yn(Se(c),r),n.flags|=2),!Xi(n))if(l&8)Cl(e,t,n.child,r,o,i,!1),Wn(t,e,a,o,c,n,i,r);else if(l&32){let d=vl(n,r),u;for(;u=d();)Wn(t,e,a,o,u,n,i,r);Wn(t,e,a,o,c,n,i,r)}else l&16?Rp(e,t,r,n,o,i):Wn(t,e,a,o,c,n,i,r);n=s?n.projectionNext:n.next}}function ts(e,t,n,r,o,i){Cl(n,r,e.firstChild,t,o,i,!1)}function mb(e,t,n){let r=t[q],o=Sp(e,n,t),i=n.parent||t[ye],s=Ap(i,n,t);Rp(r,0,t,n,o,s)}function Rp(e,t,n,r,o,i){let s=n[Ee],c=s[ye].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Wn(t,e,n[At],o,d,r,i,n)}else{let l=c,d=s[re];cp(r)&&(l.flags|=128),Cl(e,t,l,d,o,i,!0)}}function hb(e,t,n,r,o,i,s){let a=r[Ft],c=Se(r);a!==c&&Wn(t,e,n,i,a,o,s);for(let l=he;l<r.length;l++){let d=r[l];ts(d[D],d,e,t,i,a)}}function gb(e,t,n,r,o){if(t)o?e.addClass(n,r):e.removeClass(n,r);else{let i=r.indexOf("-")===-1?void 0:rt.DashCase;o==null?e.removeStyle(n,r,i):(typeof o=="string"&&o.endsWith("!important")&&(o=o.slice(0,-10),i|=rt.Important),e.setStyle(n,r,o,i))}}function Fp(e,t,n,r,o){let i=Ot(),s=r&2;try{kt(-1),s&&t.length>Q&&Ip(e,t,Q,!1);let a=s?A.TemplateUpdateStart:A.TemplateCreateStart;R(a,o,n),n(r,o)}finally{kt(i);let a=s?A.TemplateUpdateEnd:A.TemplateCreateEnd;R(a,o,n)}}function wl(e,t,n){Cb(e,t,n),(n.flags&64)===64&&wb(e,t,n)}function ns(e,t,n=Ve){let r=t.localNames;if(r!==null){let o=t.index+1;for(let i=0;i<r.length;i+=2){let s=r[i+1],a=s===-1?n(t,e):e[s];e[o++]=a}}}function vb(e,t,n,r){let i=r.get(mp,pp)||n===He.ShadowDom||n===He.ExperimentalIsolatedShadowDom,s=e.selectRootElement(t,i);return yb(s),s}function yb(e){bb(e)}var bb=()=>null;function _b(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function Db(e,t,n,r,o,i){let s=t[D];if(Il(e,s,t,n,r)){ht(e)&&Eb(t,e.index);return}e.type&3&&(n=_b(n)),Op(e,t,n,r,o,i)}function Op(e,t,n,r,o,i){if(e.type&3){let s=Ve(e,t);r=i!=null?i(r,e.value||"",n):r,o.setProperty(s,n,r)}else e.type&12}function Eb(e,t){let n=Ae(t,e);n[E]&16||(n[E]|=64)}function Cb(e,t,n){let r=n.directiveStart,o=n.directiveEnd;ht(n)&&Jy(t,n,e.data[r+n.componentOffset]),e.firstCreatePass||Bi(n,t);let i=n.initialInputs;for(let s=r;s<o;s++){let a=e.data[s],c=Gr(t,e,s,n);if(Yn(c,t),i!==null&&Tb(t,s-r,c,a,n,i),et(a)){let l=Ae(n.index,t);l[ue]=Gr(t,e,s,n)}}}function wb(e,t,n){let r=n.directiveStart,o=n.directiveEnd,i=n.index,s=tf();try{kt(i);for(let a=r;a<o;a++){let c=e.data[a],l=t[a];wi(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&Ib(c,l)}}finally{kt(-1),wi(s)}}function Ib(e,t){e.hostBindings!==null&&e.hostBindings(1,t)}function kp(e,t){let n=e.directiveRegistry,r=null;if(n)for(let o=0;o<n.length;o++){let i=n[o];Ep(t,i.selectors,!1)&&(r??=[],et(i)?r.unshift(i):r.push(i))}return r}function xb(e,t,n,r,o,i){let s=Ve(e,t);Mb(t[q],s,i,e.value,n,r,o)}function Mb(e,t,n,r,o,i,s){if(i==null)e.removeAttribute(t,o,n);else{let a=s==null?Ga(i):s(i,r||"",o);e.setAttribute(t,o,a,n)}}function Tb(e,t,n,r,o,i){let s=i[t];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Uc(r,n,c,l)}}function Pp(e,t,n,r,o){let i=Q+n,s=t[D],a=o(s,t,e,r,n);t[i]=a,zn(e,!0);let c=e.type===2;return c?(_p(t[q],a,e),(Zu()===0||Lr(e))&&Yn(a,t),Yu()):Yn(a,t),Si()&&(!c||!Xi(e))&&El(s,t,a,e),e}function Lp(e){let t=e;return hc()?gc():(t=t.parent,zn(t,!1)),t}function Sb(e,t){let n=e[At];if(!n)return;let r;try{r=n.get(Lt,null)}catch{r=null}r?.(t)}function Il(e,t,n,r,o){let i=e.inputs?.[r],s=e.hostDirectiveInputs?.[r],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=t.data[l];Uc(u,n[l],d,o),a=!0}if(i)for(let c of i){let l=n[c],d=t.data[c];Uc(d,l,r,o),a=!0}return a}function Ab(e,t){let n=Ae(t,e),r=n[D];Nb(r,n);let o=n[Pe];o!==null&&n[jn]===null&&(n[jn]=hp(o,n[At])),R(A.ComponentStart);try{xl(r,n,n[ue])}finally{R(A.ComponentEnd,n[ue])}}function Nb(e,t){for(let n=t.length;n<e.blueprint.length;n++)t.push(e.blueprint[n])}function xl(e,t,n){xi(t);try{let r=e.viewQuery;r!==null&&Bc(1,r,n);let o=e.template;o!==null&&Fp(e,t,o,1,n),e.firstCreatePass&&(e.firstCreatePass=!1),t[Je]?.finishViewCreation(e),e.staticContentQueries&&gp(e,t),e.staticViewQueries&&Bc(2,e.viewQuery,n);let i=e.components;i!==null&&Rb(t,i)}catch(r){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),r}finally{t[E]&=-5,Mi()}}function Rb(e,t){for(let n=0;n<t.length;n++)Ab(e,t[n])}function Ml(e,t,n,r){let o=w(null);try{let i=t.tView,a=e[E]&4096?4096:16,c=hl(e,i,n,a,null,t,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),l=e[t.index];c[Nt]=l;let d=e[Je];return d!==null&&(c[Je]=d.createEmbeddedView(i)),xl(i,c,n),c}finally{w(o)}}function zi(e,t){return!t||t.firstChild===null||cp(e)}function Wr(e,t,n,r,o=!1){for(;n!==null;){if(n.type===128){n=o?n.projectionNext:n.next;continue}let i=t[n.index];i!==null&&r.push(Se(i)),Le(i)&&Vp(i,r);let s=n.type;if(s&8)Wr(e,t,n.child,r);else if(s&32){let a=vl(n,t),c;for(;c=a();)r.push(c)}else if(s&16){let a=Np(t,n);if(Array.isArray(a))r.push(...a);else{let c=xt(t[Ee]);Wr(c[D],c,a,r,!0)}}n=o?n.projectionNext:n.next}return r}function Vp(e,t){for(let n=he;n<e.length;n++){let r=e[n],o=r[D].firstChild;o!==null&&Wr(r[D],r,o,t)}e[Ft]!==e[Pe]&&t.push(e[Ft])}function jp(e){if(e[cn]!==null){for(let t of e[cn])t.impl.addSequence(t);e[cn].length=0}}var Bp=[];function Fb(e){return e[we]??Ob(e)}function Ob(e){let t=Bp.pop()??Object.create(Pb);return t.lView=e,t}function kb(e){e.lView[we]!==e&&(e.lView=null,Bp.push(e))}var Pb=P(I({},Wt),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{un(e.lView)},consumerOnSignalRead(){this.lView[we]=this}});function Lb(e){let t=e[we]??Object.create(Vb);return t.lView=e,t}var Vb=P(I({},Wt),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let t=xt(e.lView);for(;t&&!Hp(t[D]);)t=xt(t);t&&ic(t)},consumerOnSignalRead(){this.lView[we]=this}});function Hp(e){return e.type!==2}function Up(e){if(e[It]===null)return;let t=!0;for(;t;){let n=!1;for(let r of e[It])r.dirty&&(n=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()));t=n&&!!(e[E]&8192)}}var jb=100;function $p(e,t=0){let r=e[Xe].rendererFactory,o=!1;o||r.begin?.();try{Bb(e,t)}finally{o||r.end?.()}}function Bb(e,t){let n=vc();try{Sr(!0),Wc(e,t);let r=0;for(;Vr(e);){if(r===jb)throw new _(103,!1);r++,Wc(e,1)}}finally{Sr(n)}}function Hb(e,t,n,r){if(dn(t))return;let o=t[E],i=!1,s=!1;xi(t);let a=!0,c=null,l=null;i||(Hp(e)?(l=Fb(t),c=Dt(l)):So()===null?(a=!1,l=Lb(t),c=Dt(l)):t[we]&&(Et(t[we]),t[we]=null));try{oc(t),Xu(e.bindingStartIndex),n!==null&&Fp(e,t,n,2,r);let d=(o&3)===3;if(!i)if(d){let f=e.preOrderCheckHooks;f!==null&&Fi(t,f,null)}else{let f=e.preOrderHooks;f!==null&&Oi(t,f,0,null),xc(t,0)}if(s||Ub(t),Up(t),zp(t,0),e.contentQueries!==null&&gp(e,t),!i)if(d){let f=e.contentCheckHooks;f!==null&&Fi(t,f)}else{let f=e.contentHooks;f!==null&&Oi(t,f,1),xc(t,1)}zb(e,t);let u=e.components;u!==null&&Wp(t,u,0);let p=e.viewQuery;if(p!==null&&Bc(2,p,r),!i)if(d){let f=e.viewCheckHooks;f!==null&&Fi(t,f)}else{let f=e.viewHooks;f!==null&&Oi(t,f,2),xc(t,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),t[yi]){for(let f of t[yi])f();t[yi]=null}i||(jp(t),t[E]&=-73)}catch(d){throw i||un(t),d}finally{l!==null&&(qt(l,c),a&&kb(l)),Mi()}}function zp(e,t){for(let n=dp(e);n!==null;n=up(n))for(let r=he;r<n.length;r++){let o=n[r];Gp(o,t)}}function Ub(e){for(let t=dp(e);t!==null;t=up(t)){if(!(t[E]&2))continue;let n=t[ln];for(let r=0;r<n.length;r++){let o=n[r];ic(o)}}}function $b(e,t,n){R(A.ComponentStart);let r=Ae(t,e);try{Gp(r,n)}finally{R(A.ComponentEnd,r[ue])}}function Gp(e,t){_i(e)&&Wc(e,t)}function Wc(e,t){let r=e[D],o=e[E],i=e[we],s=!!(t===0&&o&16);if(s||=!!(o&64&&t===0),s||=!!(o&1024),s||=!!(i?.dirty&&xn(i)),s||=!1,i&&(i.dirty=!1),e[E]&=-9217,s)Hb(r,e,r.template,e[ue]);else if(o&8192){let a=w(null);try{Up(e),zp(e,1);let c=r.components;c!==null&&Wp(e,c,1),jp(e)}finally{w(a)}}}function Wp(e,t,n){for(let r=0;r<t.length;r++)$b(e,t[r],n)}function zb(e,t){let n=e.hostBindingOpCodes;if(n!==null)try{for(let r=0;r<n.length;r++){let o=n[r];if(o<0)kt(~o);else{let i=o,s=n[++r],a=n[++r];ef(s,i);let c=t[i];R(A.HostBindingsUpdateStart,c);try{a(2,c)}finally{R(A.HostBindingsUpdateEnd,c)}}}}finally{kt(-1)}}function Tl(e,t){let n=vc()?64:1088;for(e[Xe].changeDetectionScheduler?.notify(t);e;){e[E]|=n;let r=xt(e);if($n(e)&&!r)return e;e=r}return null}function qp(e,t,n,r){return[e,!0,0,t,null,r,null,n,null,null]}function Gb(e,t){let n=he+t;if(n<e.length)return e[n]}function Sl(e,t,n,r=!0){let o=t[D];if(qb(o,t,e,n),r){let s=Gc(n,e),a=t[q],c=a.parentNode(e[Ft]);c!==null&&sb(o,e[ye],a,t,c,s)}let i=t[jn];i!==null&&i.firstChild!==null&&(i.firstChild=null)}function Wb(e,t){let n=Gi(e,t);return n!==void 0&&Dl(n[D],n),n}function Gi(e,t){if(e.length<=he)return;let n=he+t,r=e[n];if(r){let o=r[Nt];o!==null&&o!==e&&_l(o,r),t>0&&(e[n-1][Te]=r[Te]);let i=Fr(e,he+t);ib(r[D],r);let s=i[Je];s!==null&&s.detachView(i[D]),r[re]=null,r[Te]=null,r[E]&=-129}return r}function qb(e,t,n,r){let o=he+r,i=n.length;r>0&&(n[o-1][Te]=t),r<i-he?(t[Te]=n[o],Za(n,he+r,t)):(n.push(t),t[Te]=null),t[re]=n;let s=t[Nt];s!==null&&n!==s&&Zp(s,t);let a=t[Je];a!==null&&a.insertView(e),Di(t),t[E]|=128}function Zp(e,t){let n=e[ln],r=t[re];if(mt(r))e[E]|=2;else{let o=r[re][Ee];t[Ee]!==o&&(e[E]|=2)}n===null?e[ln]=[t]:n.push(t)}var Vt=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let t=this._lView,n=t[D];return Wr(n,t,n.firstChild,[])}constructor(t,n){this._lView=t,this._cdRefInjectingView=n}get context(){return this._lView[ue]}set context(t){this._lView[ue]=t}get destroyed(){return dn(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[re];if(Le(t)){let n=t[Pr],r=n?n.indexOf(this):-1;r>-1&&(Gi(t,r),Fr(n,r))}this._attachedToViewContainer=!1}Dl(this._lView[D],this._lView)}onDestroy(t){sc(this._lView,t)}markForCheck(){Tl(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[E]&=-129}reattach(){Di(this._lView),this._lView[E]|=128}detectChanges(){this._lView[E]|=1024,$p(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new _(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=$n(this._lView),n=this._lView[Nt];n!==null&&!t&&_l(n,this._lView),Tp(this._lView[D],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new _(902,!1);this._appRef=t;let n=$n(this._lView),r=this._lView[Nt];r!==null&&!n&&Zp(r,this._lView),Di(this._lView)}};var hn=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=Zb;constructor(n,r,o){this._declarationLView=n,this._declarationTContainer=r,this.elementRef=o}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(n,r){return this.createEmbeddedViewImpl(n,r)}createEmbeddedViewImpl(n,r,o){let i=Ml(this._declarationLView,this._declarationTContainer,n,{embeddedViewInjector:r,dehydratedView:o});return new Vt(i)}}return e})();function Zb(){return rs(be(),S())}function rs(e,t){return e.type&4?new hn(t,e,Jn(e,t)):null}function nr(e,t,n,r,o){let i=e.data[t];if(i===null)i=Yb(e,t,n,r,o),Ju()&&(i.flags|=32);else if(i.type&64){i.type=n,i.value=r,i.attrs=o;let s=Qu();i.injectorIndex=s===null?-1:s.injectorIndex}return zn(i,!0),i}function Yb(e,t,n,r,o){let i=mc(),s=hc(),a=s?i:i&&i.parent,c=e.data[t]=Kb(e,a,n,t,r,o);return Qb(e,c,i,s),c}function Qb(e,t,n,r){e.firstChild===null&&(e.firstChild=t),n!==null&&(r?n.child==null&&t.parent!==null&&(n.child=t):n.next===null&&(n.next=t,t.prev=n))}function Kb(e,t,n,r,o,i){let s=t?t.injectorIndex:-1,a=0;return uc()&&(a|=128),{type:n,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:o,attrs:i,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var Xb=()=>null,Jb=()=>null;function qc(e,t){return Xb(e,t)}function e_(e,t,n){return Jb(e,t,n)}var Yp=class{},os=class{},Zc=class{resolveComponentFactory(t){throw new _(917,!1)}},is=class{static NULL=new Zc},Ue=class{},gt=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>t_()}return e})();function t_(){let e=S(),t=be(),n=Ae(t.index,e);return(mt(n)?n:e)[q]}var Qp=(()=>{class e{static \u0275prov=v({token:e,providedIn:"root",factory:()=>null})}return e})();var Pi={},Yc=class{injector;parentInjector;constructor(t,n){this.injector=t,this.parentInjector=n}get(t,n,r){let o=this.injector.get(t,Pi,r);return o!==Pi||n===Pi?o:this.parentInjector.get(t,n,r)}};function Wi(e,t,n){let r=n?e.styles:null,o=n?e.classes:null,i=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")i=a;else if(i==1)o=li(o,a);else if(i==2){let c=a,l=t[++s];r=li(r,c+": "+l+";")}}n?e.styles=r:e.stylesWithoutHost=r,n?e.classes=o:e.classesWithoutHost=o}function j(e,t=0){let n=S();if(n===null)return M(e,t);let r=be();return op(r,n,ae(e),t)}function Kp(e,t,n,r,o){let i=r===null?null:{"":-1},s=o(e,n);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}o_(e,t,n,a,i,c,l)}i!==null&&r!==null&&n_(n,r,i)}function n_(e,t,n){let r=e.localNames=[];for(let o=0;o<t.length;o+=2){let i=n[t[o+1]];if(i==null)throw new _(-301,!1);r.push(t[o],i)}}function r_(e,t,n){t.componentOffset=n,(e.components??=[]).push(t.index)}function o_(e,t,n,r,o,i,s){let a=r.length,c=null;for(let p=0;p<a;p++){let f=r[p];c===null&&et(f)&&(c=f,r_(e,n,p)),Lc(Bi(n,t),e,f.type)}d_(n,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let p=0;p<a;p++){let f=r[p];f.providersResolver&&f.providersResolver(f)}let l=!1,d=!1,u=wp(e,t,a,null);a>0&&(n.directiveToIndex=new Map);for(let p=0;p<a;p++){let f=r[p];if(n.mergedAttrs=Zn(n.mergedAttrs,f.hostAttrs),s_(e,n,t,u,f),l_(u,f,o),s!==null&&s.has(f)){let[C,y]=s.get(f);n.directiveToIndex.set(f.type,[u,C+n.directiveStart,y+n.directiveStart])}else(i===null||!i.has(f))&&n.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(n.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(n.flags|=64);let h=f.type.prototype;!l&&(h.ngOnChanges||h.ngOnInit||h.ngDoCheck)&&((e.preOrderHooks??=[]).push(n.index),l=!0),!d&&(h.ngOnChanges||h.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(n.index),d=!0),u++}i_(e,n,i)}function i_(e,t,n){for(let r=t.directiveStart;r<t.directiveEnd;r++){let o=e.data[r];if(n===null||!n.has(o))Rf(0,t,o,r),Rf(1,t,o,r),Of(t,r,!1);else{let i=n.get(o);Ff(0,t,i,r),Ff(1,t,i,r),Of(t,r,!0)}}}function Rf(e,t,n,r){let o=e===0?n.inputs:n.outputs;for(let i in o)if(o.hasOwnProperty(i)){let s;e===0?s=t.inputs??={}:s=t.outputs??={},s[i]??=[],s[i].push(r),Xp(t,i)}}function Ff(e,t,n,r){let o=e===0?n.inputs:n.outputs;for(let i in o)if(o.hasOwnProperty(i)){let s=o[i],a;e===0?a=t.hostDirectiveInputs??={}:a=t.hostDirectiveOutputs??={},a[s]??=[],a[s].push(r,i),Xp(t,s)}}function Xp(e,t){t==="class"?e.flags|=8:t==="style"&&(e.flags|=16)}function Of(e,t,n){let{attrs:r,inputs:o,hostDirectiveInputs:i}=e;if(r===null||!n&&o===null||n&&i===null||pl(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<r.length;){let c=r[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!n&&o.hasOwnProperty(c)){let l=o[c];for(let d of l)if(d===t){s??=[],s.push(c,r[a+1]);break}}else if(n&&i.hasOwnProperty(c)){let l=i[c];for(let d=0;d<l.length;d+=2)if(l[d]===t){s??=[],s.push(l[d+1],r[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function s_(e,t,n,r,o){e.data[r]=o;let i=o.factory||(o.factory=tn(o.type,!0)),s=new mn(i,et(o),j,null);e.blueprint[r]=s,n[r]=s,a_(e,t,r,wp(e,n,o.hostVars,$e),o)}function a_(e,t,n,r,o){let i=o.hostBindings;if(i){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~t.index;c_(s)!=a&&s.push(a),s.push(n,r,i)}}function c_(e){let t=e.length;for(;t>0;){let n=e[--t];if(typeof n=="number"&&n<0)return n}return 0}function l_(e,t,n){if(n){if(t.exportAs)for(let r=0;r<t.exportAs.length;r++)n[t.exportAs[r]]=e;et(t)&&(n[""]=e)}}function d_(e,t,n){e.flags|=1,e.directiveStart=t,e.directiveEnd=t+n,e.providerIndexes=t}function Jp(e,t,n,r,o,i,s,a){let c=t[D],l=c.consts,d=tt(l,s),u=nr(c,e,n,r,d);return i&&Kp(c,t,u,tt(l,a),o),u.mergedAttrs=Zn(u.mergedAttrs,u.attrs),u.attrs!==null&&Wi(u,u.attrs,!1),u.mergedAttrs!==null&&Wi(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function em(e,t){Qf(e,t),nc(t)&&e.queries.elementEnd(t)}function u_(e,t,n,r,o,i){let s=t.consts,a=tt(s,o),c=nr(t,e,n,r,a);if(c.mergedAttrs=Zn(c.mergedAttrs,c.attrs),i!=null){let l=tt(s,i);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Wi(c,c.attrs,!1),c.mergedAttrs!==null&&Wi(c,c.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,c),c}function Al(e){return nm(e)?Array.isArray(e)||!(e instanceof Map)&&Symbol.iterator in e:!1}function tm(e,t){if(Array.isArray(e))for(let n=0;n<e.length;n++)t(e[n]);else{let n=e[Symbol.iterator](),r;for(;!(r=n.next()).done;)t(r.value)}}function nm(e){return e!==null&&(typeof e=="function"||typeof e=="object")}function bn(e,t,n){if(n===$e)return!1;let r=e[t];return Object.is(r,n)?!1:(e[t]=n,!0)}function Sc(e,t,n){return function r(o){let i=ht(e)?Ae(e.index,t):t;Tl(i,5);let s=t[ue],a=kf(t,s,n,o),c=r.__ngNextListenerFn__;for(;c;)a=kf(t,s,c,o)&&a,c=c.__ngNextListenerFn__;return a}}function kf(e,t,n,r){let o=w(null);try{return R(A.OutputStart,t,n),n(r)!==!1}catch(i){return Sb(e,i),!1}finally{R(A.OutputEnd,t,n),w(o)}}function f_(e,t,n,r,o,i,s,a){let c=Lr(e),l=!1,d=null;if(!r&&c&&(d=m_(t,n,i,e.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=Ve(e,n),p=r?r(u):u;Ry(n,p,i,a);let f=o.listen(p,i,a);if(!p_(i)){let h=r?C=>r(Se(C[e.index])):e.index;rm(h,t,n,i,a,f,!1)}}return l}function p_(e){return e.startsWith("animation")||e.startsWith("transition")}function m_(e,t,n,r){let o=e.cleanup;if(o!=null)for(let i=0;i<o.length-1;i+=2){let s=o[i];if(s===n&&o[i+1]===r){let a=t[Bn],c=o[i+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(i+=2)}return null}function rm(e,t,n,r,o,i,s){let a=t.firstCreatePass?cc(t):null,c=ac(n),l=c.length;c.push(o,i),a&&a.push(r,e,l,(l+1)*(s?-1:1))}function Pf(e,t,n,r,o,i){let s=t[n],a=t[D],l=a.data[n].outputs[r],u=s[l].subscribe(i);rm(e.index,a,t,o,i,u,!0)}var Qc=Symbol("BINDING");function om(e){return e.debugInfo?.className||e.type.name||null}var Kc=class extends is{ngModule;constructor(t){super(),this.ngModule=t}resolveComponentFactory(t){let n=Tt(t);return new Kn(n,this.ngModule)}};function h_(e){return Object.keys(e).map(t=>{let[n,r,o]=e[t],i={propName:n,templateName:t,isSignal:(r&Ji.SignalBased)!==0};return o&&(i.transform=o),i})}function g_(e){return Object.keys(e).map(t=>({propName:e[t],templateName:t}))}function v_(e,t,n){let r=t instanceof le?t:t?.injector;return r&&e.getStandaloneInjector!==null&&(r=e.getStandaloneInjector(r)||r),r?new Yc(n,r):n}function y_(e){let t=e.get(Ue,null);if(t===null)throw new _(407,!1);let n=e.get(Qp,null),r=e.get(Ke,null),o=e.get(ot,null,{optional:!0});return{rendererFactory:t,sanitizer:n,changeDetectionScheduler:r,ngReflect:!1,tracingService:o}}function b_(e,t){let n=im(e);return yp(t,n,n==="svg"?Bu:n==="math"?Hu:null)}function im(e){return(e.selectors[0][0]||"div").toLowerCase()}var Kn=class extends os{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=h_(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=g_(this.componentDef.outputs),this.cachedOutputs}constructor(t,n){super(),this.componentDef=t,this.ngModule=n,this.componentType=t.type,this.selector=Yy(t.selectors),this.ngContentSelectors=t.ngContentSelectors??[],this.isBoundToModule=!!n}create(t,n,r,o,i,s){R(A.DynamicComponentStart);let a=w(null);try{let c=this.componentDef,l=v_(c,o||this.ngModule,t),d=y_(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(om(c),()=>this.createComponentRef(d,l,n,r,i,s)):this.createComponentRef(d,l,n,r,i,s)}finally{w(a)}}createComponentRef(t,n,r,o,i,s){let a=this.componentDef,c=__(o,a,s,i),l=t.rendererFactory.createRenderer(null,a),d=o?vb(l,o,a.encapsulation,n):b_(a,l),u=s?.some(Lf)||i?.some(h=>typeof h!="function"&&h.bindings.some(Lf)),p=hl(null,c,null,512|Cp(a),null,null,t,l,n,null,hp(d,n,!0));p[Q]=d,xi(p);let f=null;try{let h=Jp(Q,p,2,"#host",()=>c.directiveRegistry,!0,0);_p(l,d,h),Yn(d,p),wl(c,p,h),vp(c,h,p),em(c,h),r!==void 0&&E_(h,this.ngContentSelectors,r),f=Ae(h.index,p),p[ue]=f[ue],xl(c,p,null)}catch(h){throw f!==null&&jc(f),jc(p),h}finally{R(A.DynamicComponentEnd),Mi()}return new qi(this.componentType,p,!!u)}};function __(e,t,n,r){let o=e?["ng-version","21.2.5"]:Qy(t.selectors[0]),i=null,s=null,a=0;if(n)for(let d of n)a+=d[Qc].requiredVars,d.create&&(d.targetIdx=0,(i??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(r)for(let d=0;d<r.length;d++){let u=r[d];if(typeof u!="function")for(let p of u.bindings){a+=p[Qc].requiredVars;let f=d+1;p.create&&(p.targetIdx=f,(i??=[]).push(p)),p.update&&(p.targetIdx=f,(s??=[]).push(p))}}let c=[t];if(r)for(let d of r){let u=typeof d=="function"?d:d.type,p=$a(u);c.push(p)}return ml(0,null,D_(i,s),1,a,c,null,null,null,[o],null)}function D_(e,t){return!e&&!t?null:n=>{if(n&1&&e)for(let r of e)r.create();if(n&2&&t)for(let r of t)r.update()}}function Lf(e){let t=e[Qc].kind;return t==="input"||t==="twoWay"}var qi=class extends Yp{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(t,n,r){super(),this._rootLView=n,this._hasInputBindings=r,this._tNode=bi(n[D],Q),this.location=Jn(this._tNode,n),this.instance=Ae(this._tNode.index,n)[ue],this.hostView=this.changeDetectorRef=new Vt(n,void 0),this.componentType=t}setInput(t,n){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),n))return;let o=this._rootLView,i=Il(r,o[D],o,t,n);this.previousInputValues.set(t,n);let s=Ae(r.index,o);Tl(s,1)}get injector(){return new pn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function E_(e,t,n){let r=e.projection=[];for(let o=0;o<t.length;o++){let i=n[o];r.push(i!=null&&i.length?Array.from(i):null)}}var _n=(()=>{class e{static __NG_ELEMENT_ID__=C_}return e})();function C_(){let e=be();return sm(e,S())}var Xc=class e extends _n{_lContainer;_hostTNode;_hostLView;constructor(t,n,r){super(),this._lContainer=t,this._hostTNode=n,this._hostLView=r}get element(){return Jn(this._hostTNode,this._hostLView)}get injector(){return new pn(this._hostTNode,this._hostLView)}get parentInjector(){let t=cl(this._hostTNode,this._hostLView);if(Xf(t)){let n=ji(t,this._hostLView),r=Vi(t),o=n[D].data[r+8];return new pn(o,n)}else return new pn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let n=Vf(this._lContainer);return n!==null&&n[t]||null}get length(){return this._lContainer.length-he}createEmbeddedView(t,n,r){let o,i;typeof r=="number"?o=r:r!=null&&(o=r.index,i=r.injector);let s=qc(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(n||{},i,s);return this.insertImpl(a,o,zi(this._hostTNode,s)),a}createComponent(t,n,r,o,i,s,a){let c=t&&!iy(t),l;if(c)l=n;else{let y=n||{};l=y.index,r=y.injector,o=y.projectableNodes,i=y.environmentInjector||y.ngModuleRef,s=y.directives,a=y.bindings}let d=c?t:new Kn(Tt(t)),u=r||this.parentInjector;if(!i&&d.ngModule==null){let b=(c?u:this.parentInjector).get(le,null);b&&(i=b)}let p=Tt(d.componentType??{}),f=qc(this._lContainer,p?.id??null),h=f?.firstChild??null,C=d.create(u,o,h,i,s,a);return this.insertImpl(C.hostView,l,zi(this._hostTNode,f)),C}insert(t,n){return this.insertImpl(t,n,!0)}insertImpl(t,n,r){let o=t._lView;if(zu(o)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let c=o[re],l=new e(c,c[ye],c[re]);l.detach(l.indexOf(t))}}let i=this._adjustIndex(n),s=this._lContainer;return Sl(s,o,i,r),t.attachToViewContainerRef(),Za(Ac(s),i,t),t}move(t,n){return this.insert(t,n)}indexOf(t){let n=Vf(this._lContainer);return n!==null?n.indexOf(t):-1}remove(t){let n=this._adjustIndex(t,-1),r=Gi(this._lContainer,n);r&&(Fr(Ac(this._lContainer),n),Dl(r[D],r))}detach(t){let n=this._adjustIndex(t,-1),r=Gi(this._lContainer,n);return r&&Fr(Ac(this._lContainer),n)!=null?new Vt(r):null}_adjustIndex(t,n=0){return t??this.length+n}};function Vf(e){return e[Pr]}function Ac(e){return e[Pr]||(e[Pr]=[])}function sm(e,t){let n,r=t[e.index];return Le(r)?n=r:(n=qp(r,t,null,e),t[e.index]=n,gl(t,n)),I_(n,t,e,r),new Xc(n,e,t)}function w_(e,t){let n=e[q],r=n.createComment(""),o=Ve(t,e),i=n.parentNode(o);return Ui(n,i,r,n.nextSibling(o),!1),r}var I_=T_,x_=()=>!1;function M_(e,t,n){return x_(e,t,n)}function T_(e,t,n,r){if(e[Ft])return;let o;n.type&8?o=Se(r):o=w_(t,n),e[Ft]=o}var Jc=class e{queryList;matches=null;constructor(t){this.queryList=t}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},el=class e{queries;constructor(t=[]){this.queries=t}createEmbeddedView(t){let n=t.queries;if(n!==null){let r=t.contentQueries!==null?t.contentQueries[0]:n.length,o=[];for(let i=0;i<r;i++){let s=n.getByIndex(i),a=this.queries[s.indexInDeclarationView];o.push(a.clone())}return new e(o)}return null}insertView(t){this.dirtyQueriesWithMatches(t)}detachView(t){this.dirtyQueriesWithMatches(t)}finishViewCreation(t){this.dirtyQueriesWithMatches(t)}dirtyQueriesWithMatches(t){for(let n=0;n<this.queries.length;n++)Rl(t,n).matches!==null&&this.queries[n].setDirty()}},Zi=class{flags;read;predicate;constructor(t,n,r=null){this.flags=n,this.read=r,typeof t=="string"?this.predicate=F_(t):this.predicate=t}},tl=class e{queries;constructor(t=[]){this.queries=t}elementStart(t,n){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(t,n)}elementEnd(t){for(let n=0;n<this.queries.length;n++)this.queries[n].elementEnd(t)}embeddedTView(t){let n=null;for(let r=0;r<this.length;r++){let o=n!==null?n.length:0,i=this.getByIndex(r).embeddedTView(t,o);i&&(i.indexInDeclarationView=r,n!==null?n.push(i):n=[i])}return n!==null?new e(n):null}template(t,n){for(let r=0;r<this.queries.length;r++)this.queries[r].template(t,n)}getByIndex(t){return this.queries[t]}get length(){return this.queries.length}track(t){this.queries.push(t)}},nl=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(t,n=-1){this.metadata=t,this._declarationNodeIndex=n}elementStart(t,n){this.isApplyingToNode(n)&&this.matchTNode(t,n)}elementEnd(t){this._declarationNodeIndex===t.index&&(this._appliesToNextNode=!1)}template(t,n){this.elementStart(t,n)}embeddedTView(t,n){return this.isApplyingToNode(t)?(this.crossesNgTemplate=!0,this.addMatch(-t.index,n),new e(this.metadata)):null}isApplyingToNode(t){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let n=this._declarationNodeIndex,r=t.parent;for(;r!==null&&r.type&8&&r.index!==n;)r=r.parent;return n===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(t,n){let r=this.metadata.predicate;if(Array.isArray(r))for(let o=0;o<r.length;o++){let i=r[o];this.matchTNodeWithReadOption(t,n,S_(n,i)),this.matchTNodeWithReadOption(t,n,ki(n,t,i,!1,!1))}else r===hn?n.type&4&&this.matchTNodeWithReadOption(t,n,-1):this.matchTNodeWithReadOption(t,n,ki(n,t,r,!1,!1))}matchTNodeWithReadOption(t,n,r){if(r!==null){let o=this.metadata.read;if(o!==null)if(o===K||o===_n||o===hn&&n.type&4)this.addMatch(n.index,-2);else{let i=ki(n,t,o,!1,!1);i!==null&&this.addMatch(n.index,i)}else this.addMatch(n.index,r)}}addMatch(t,n){this.matches===null?this.matches=[t,n]:this.matches.push(t,n)}};function S_(e,t){let n=e.localNames;if(n!==null){for(let r=0;r<n.length;r+=2)if(n[r]===t)return n[r+1]}return null}function A_(e,t){return e.type&11?Jn(e,t):e.type&4?rs(e,t):null}function N_(e,t,n,r){return n===-1?A_(t,e):n===-2?R_(e,t,r):Gr(e,e[D],n,t)}function R_(e,t,n){if(n===K)return Jn(t,e);if(n===hn)return rs(t,e);if(n===_n)return sm(t,e)}function am(e,t,n,r){let o=t[Je].queries[r];if(o.matches===null){let i=e.data,s=n.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=i[l];a.push(N_(t,d,s[c+1],n.metadata.read))}}o.matches=a}return o.matches}function rl(e,t,n,r){let o=e.queries.getByIndex(n),i=o.matches;if(i!==null){let s=am(e,t,o,n);for(let a=0;a<i.length;a+=2){let c=i[a];if(c>0)r.push(s[a/2]);else{let l=i[a+1],d=t[-c];for(let u=he;u<d.length;u++){let p=d[u];p[Nt]===p[re]&&rl(p[D],p,l,r)}if(d[ln]!==null){let u=d[ln];for(let p=0;p<u.length;p++){let f=u[p];rl(f[D],f,l,r)}}}}}return r}function Nl(e,t){return e[Je].queries[t].queryList}function cm(e,t,n){let r=new Hi((n&4)===4);return qu(e,t,r,r.destroy),(t[Je]??=new el).queries.push(new Jc(r))-1}function lm(e,t,n){let r=ee();return r.firstCreatePass&&(um(r,new Zi(e,t,n),-1),(t&2)===2&&(r.staticViewQueries=!0)),cm(r,S(),t)}function dm(e,t,n,r){let o=ee();if(o.firstCreatePass){let i=be();um(o,new Zi(t,n,r),i.index),O_(o,e),(n&2)===2&&(o.staticContentQueries=!0)}return cm(o,S(),n)}function F_(e){return e.split(",").map(t=>t.trim())}function um(e,t,n){e.queries===null&&(e.queries=new tl),e.queries.track(new nl(t,n))}function O_(e,t){let n=e.contentQueries||(e.contentQueries=[]),r=n.length?n[n.length-1]:-1;t!==r&&n.push(e.queries.length-1,t)}function Rl(e,t){return e.queries.getByIndex(t)}function fm(e,t){let n=e[D],r=Rl(n,t);return r.crossesNgTemplate?rl(n,e,t,[]):am(n,e,r,t)}function pm(e,t,n){let r,o=_r(()=>{r._dirtyCounter();let i=k_(r,e);if(t&&i===void 0)throw new _(-951,!1);return i});return r=o[ie],r._dirtyCounter=Ne(0),r._flatValue=void 0,o}function Fl(e){return pm(!0,!1,e)}function Ol(e){return pm(!0,!0,e)}function mm(e,t){let n=e[ie];n._lView=S(),n._queryIndex=t,n._queryList=Nl(n._lView,t),n._queryList.onDirty(()=>n._dirtyCounter.update(r=>r+1))}function k_(e,t){let n=e._lView,r=e._queryIndex;if(n===void 0||r===void 0||n[E]&4)return t?void 0:me;let o=Nl(n,r),i=fm(n,r);return o.reset(i,ap),t?o.first:o._changesDetected||e._flatValue===void 0?e._flatValue=o.toArray():e._flatValue}var qr=class{};var Zr=class extends qr{injector;componentFactoryResolver=new Kc(this);instance=null;constructor(t){super();let n=new rn([...t.providers,{provide:qr,useValue:this},{provide:is,useValue:this.componentFactoryResolver}],t.parent||Ln(),t.debugName,new Set(["environment"]));this.injector=n,t.runEnvironmentInitializers&&n.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function hm(e,t,n=null){return new Zr({providers:e,parent:t,debugName:n,runEnvironmentInitializers:!0}).injector}var P_=(()=>{class e{_injector;cachedInjectors=new Map;constructor(n){this._injector=n}getOrCreateStandaloneInjector(n){if(!n.standalone)return null;if(!this.cachedInjectors.has(n)){let r=Ka(!1,n.type),o=r.length>0?hm([r],this._injector,""):null;this.cachedInjectors.set(n,o)}return this.cachedInjectors.get(n)}ngOnDestroy(){try{for(let n of this.cachedInjectors.values())n!==null&&n.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=v({token:e,providedIn:"environment",factory:()=>new e(M(le))})}return e})();function oe(e){return Qr(()=>{let t=gm(e),n=P(I({},t),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===ll.OnPush,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&e.dependencies||null,getStandaloneInjector:t.standalone?o=>o.get(P_).getOrCreateStandaloneInjector(n):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||He.Emulated,styles:e.styles||me,_:null,schemas:e.schemas||null,tView:null,id:""});t.standalone&&Kr("NgStandalone"),vm(n);let r=e.dependencies;return n.directiveDefs=jf(r,L_),n.pipeDefs=jf(r,Mu),n.id=B_(n),n})}function L_(e){return Tt(e)||$a(e)}function $(e){return Qr(()=>({type:e.type,bootstrap:e.bootstrap||me,declarations:e.declarations||me,imports:e.imports||me,exports:e.exports||me,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function V_(e,t){if(e==null)return St;let n={};for(let r in e)if(e.hasOwnProperty(r)){let o=e[r],i,s,a,c;Array.isArray(o)?(a=o[0],i=o[1],s=o[2]??i,c=o[3]||null):(i=o,s=o,a=Ji.None,c=null),n[i]=[r,a,c],t[i]=s}return n}function j_(e){if(e==null)return St;let t={};for(let n in e)e.hasOwnProperty(n)&&(t[e[n]]=n);return t}function O(e){return Qr(()=>{let t=gm(e);return vm(t),t})}function gm(e){let t={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:t,inputConfig:e.inputs||St,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||me,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:V_(e.inputs,t),outputs:j_(e.outputs),debugInfo:null}}function vm(e){e.features?.forEach(t=>t(e))}function jf(e,t){return e?()=>{let n=typeof e=="function"?e():e,r=[];for(let o of n){let i=t(o);i!==null&&r.push(i)}return r}:null}function B_(e){let t=0,n=typeof e.consts=="function"?"":e.consts,r=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,n,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let i of r.join("|"))t=Math.imul(31,t)+i.charCodeAt(0)<<0;return t+=2147483648,"c"+t}function H_(e){return Object.getPrototypeOf(e.prototype).constructor}function Ie(e){let t=H_(e.type),n=!0,r=[e];for(;t;){let o;if(et(e))o=t.\u0275cmp||t.\u0275dir;else{if(t.\u0275cmp)throw new _(903,!1);o=t.\u0275dir}if(o){if(n){r.push(o);let s=e;s.inputs=Nc(e.inputs),s.declaredInputs=Nc(e.declaredInputs),s.outputs=Nc(e.outputs);let a=o.hostBindings;a&&W_(e,a);let c=o.viewQuery,l=o.contentQueries;if(c&&z_(e,c),l&&G_(e,l),U_(e,o),xu(e.outputs,o.outputs),et(o)&&o.data.animation){let d=e.data;d.animation=(d.animation||[]).concat(o.data.animation)}}let i=o.features;if(i)for(let s=0;s<i.length;s++){let a=i[s];a&&a.ngInherit&&a(e),a===Ie&&(n=!1)}}t=Object.getPrototypeOf(t)}$_(r)}function U_(e,t){for(let n in t.inputs){if(!t.inputs.hasOwnProperty(n)||e.inputs.hasOwnProperty(n))continue;let r=t.inputs[n];r!==void 0&&(e.inputs[n]=r,e.declaredInputs[n]=t.declaredInputs[n])}}function $_(e){let t=0,n=null;for(let r=e.length-1;r>=0;r--){let o=e[r];o.hostVars=t+=o.hostVars,o.hostAttrs=Zn(o.hostAttrs,n=Zn(n,o.hostAttrs))}}function Nc(e){return e===St?{}:e===me?[]:e}function z_(e,t){let n=e.viewQuery;n?e.viewQuery=(r,o)=>{t(r,o),n(r,o)}:e.viewQuery=t}function G_(e,t){let n=e.contentQueries;n?e.contentQueries=(r,o,i)=>{t(r,o,i),n(r,o,i)}:e.contentQueries=t}function W_(e,t){let n=e.hostBindings;n?e.hostBindings=(r,o)=>{t(r,o),n(r,o)}:e.hostBindings=t}function ym(e,t,n,r,o,i,s,a){if(n.firstCreatePass){e.mergedAttrs=Zn(e.mergedAttrs,e.attrs);let d=e.tView=ml(2,e,o,i,s,n.directiveRegistry,n.pipeRegistry,null,n.schemas,n.consts,null);n.queries!==null&&(n.queries.template(n,e),d.queries=n.queries.embeddedTView(e))}a&&(e.flags|=a),zn(e,!1);let c=Z_(n,t,e,r);Si()&&El(n,t,c,e),Yn(c,t);let l=qp(c,t,c,e);t[r+Q]=l,gl(t,l),M_(l,e,t)}function q_(e,t,n,r,o,i,s,a,c,l,d){let u=n+Q,p;return t.firstCreatePass?(p=nr(t,u,4,s||null,a||null),dc()&&Kp(t,e,p,tt(t.consts,l),kp),Qf(t,p)):p=t.data[u],ym(p,e,t,n,r,o,i,c),Lr(p)&&wl(t,e,p),l!=null&&ns(e,p,d),p}function kl(e,t,n,r,o,i,s,a,c,l,d){let u=n+Q,p;if(t.firstCreatePass){if(p=nr(t,u,4,s||null,a||null),l!=null){let f=tt(t.consts,l);p.localNames=[];for(let h=0;h<f.length;h+=2)p.localNames.push(f[h],-1)}}else p=t.data[u];return ym(p,e,t,n,r,o,i,c),l!=null&&ns(e,p,d),p}function jt(e,t,n,r,o,i,s,a){let c=S(),l=ee(),d=tt(l.consts,i);return q_(c,l,e,t,n,r,o,d,void 0,s,a),jt}var Z_=Y_;function Y_(e,t,n,r){return Ai(!0),t[q].createComment("")}var Pl=new g("");function rr(e){return!!e&&typeof e.then=="function"}function Ll(e){return!!e&&typeof e.subscribe=="function"}var bm=new g("");var Vl=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((n,r)=>{this.resolve=n,this.reject=r});appInits=m(bm,{optional:!0})??[];injector=m(de);constructor(){}runInitializers(){if(this.initialized)return;let n=[];for(let o of this.appInits){let i=Vn(this.injector,o);if(rr(i))n.push(i);else if(Ll(i)){let s=new Promise((a,c)=>{i.subscribe({complete:a,error:c})});n.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(n).then(()=>{r()}).catch(o=>{this.reject(o)}),n.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),_m=new g("");function Dm(){ea(()=>{let e="";throw new _(600,e)})}function Em(e){return e.isBoundToModule}var Q_=10;var Dn=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=m(Lt);afterRenderManager=m(yl);zonelessEnabled=m(Hr);rootEffectScheduler=m(Ni);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new G;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=m(fn);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(pe(n=>!n))}constructor(){m(ot,{optional:!0})}whenStable(){let n;return new Promise(r=>{n=this.isStable.subscribe({next:o=>{o&&r()}})}).finally(()=>{n.unsubscribe()})}_injector=m(le);_rendererFactory=null;get injector(){return this._injector}bootstrap(n,r){return this.bootstrapImpl(n,r)}bootstrapImpl(n,r,o=de.NULL){return this._injector.get(N).run(()=>{R(A.BootstrapComponentStart);let s=n instanceof os;if(!this._injector.get(Vl).done){let h="";throw new _(405,h)}let c;s?c=n:c=this._injector.get(is).resolveComponentFactory(n),this.componentTypes.push(c.componentType);let l=Em(c)?void 0:this._injector.get(qr),d=r||c.selector,u=c.create(o,[],d,l),p=u.location.nativeElement,f=u.injector.get(Pl,null);return f?.registerApplication(p),u.onDestroy(()=>{this.detachView(u.hostView),zr(this.components,u),f?.unregisterApplication(p)}),this._loadComponent(u),R(A.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){R(A.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(es.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw R(A.ChangeDetectionEnd),new _(101,!1);let n=w(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,w(n),this.afterTick.next(),R(A.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ue,null,{optional:!0}));let n=0;for(;this.dirtyFlags!==0&&n++<Q_;){R(A.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{R(A.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let n=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:o}of this.allViews){if(!r&&!Vr(o))continue;let i=r&&!this.zonelessEnabled?0:1;$p(o,i),n=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}n||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:n})=>Vr(n))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(n){let r=n;this._views.push(r),r.attachToAppRef(this)}detachView(n){let r=n;zr(this._views,r),r.detachFromAppRef()}_loadComponent(n){this.attachView(n.hostView);try{this.tick()}catch(o){this.internalErrorHandler(o)}this.components.push(n),this._injector.get(_m,[]).forEach(o=>o(n))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(n=>n()),this._views.slice().forEach(n=>n.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(n){return this._destroyListeners.push(n),()=>zr(this._destroyListeners,n)}destroy(){if(this._destroyed)throw new _(406,!1);let n=this._injector;n.destroy&&!n.destroyed&&n.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function zr(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function it(e,t,n,r){let o=S(),i=Gn();if(bn(o,i,t)){let s=ee(),a=Ti();xb(a,o,e,t,n,r)}return it}function _e(e,t,n,r,o,i,s,a){Kr("NgControlFlow");let c=S(),l=ee(),d=tt(l.consts,i);return kl(c,l,e,t,n,r,o,d,256,s,a),jl}function jl(e,t,n,r,o,i,s,a){Kr("NgControlFlow");let c=S(),l=ee(),d=tt(l.consts,i);return kl(c,l,e,t,n,r,o,d,512,s,a),jl}function De(e,t){Kr("NgControlFlow");let n=S(),r=Gn(),o=n[r]!==$e?n[r]:-1,i=o!==-1?Bf(n,Q+o):void 0,s=0;if(bn(n,r,e)){let a=w(null);try{if(i!==void 0&&Wb(i,s),e!==-1){let c=Q+e,l=Bf(n,c),d=K_(n[D],c),u=e_(l,d,n),p=Ml(n,d,t,{dehydratedView:u});Sl(l,p,s,zi(d,u))}}finally{w(a)}}else if(i!==void 0){let a=Gb(i,s);a!==void 0&&(a[ue]=t)}}function Bf(e,t){return e[t]}function K_(e,t){return bi(e,t)}function ze(e,t,n){let r=S(),o=Gn();if(bn(r,o,t)){let i=ee(),s=Ti();Db(s,r,e,t,r[q],n)}return ze}function ol(e,t,n,r,o){Il(t,e,n,o?"class":"style",r)}function B(e,t,n,r){let o=S(),i=o[D],s=e+Q,a=i.firstCreatePass?Jp(s,o,2,t,kp,dc(),n,r):i.data[s];if(ht(a)){let c=o[Xe].tracingService;if(c&&c.componentCreate){let l=i.data[a.directiveStart+a.componentOffset];return c.componentCreate(om(l),()=>(Hf(e,t,o,a,r),B))}}return Hf(e,t,o,a,r),B}function Hf(e,t,n,r,o){if(Pp(r,n,e,t,Cm),Lr(r)){let i=n[D];wl(i,n,r),vp(i,r,n)}o!=null&&ns(n,r)}function H(){let e=ee(),t=be(),n=Lp(t);return e.firstCreatePass&&em(e,n),fc(n)&&pc(),lc(),n.classesWithoutHost!=null&&uy(n)&&ol(e,n,S(),n.classesWithoutHost,!0),n.stylesWithoutHost!=null&&fy(n)&&ol(e,n,S(),n.stylesWithoutHost,!1),H}function Re(e,t,n,r){return B(e,t,n,r),H(),Re}function Bt(e,t,n,r){let o=S(),i=o[D],s=e+Q,a=i.firstCreatePass?u_(s,i,2,t,n,r):i.data[s];return Pp(a,o,e,t,Cm),r!=null&&ns(o,a),Bt}function Ht(){let e=be(),t=Lp(e);return fc(t)&&pc(),lc(),Ht}function vt(e,t,n,r){return Bt(e,t,n,r),Ht(),vt}var Cm=(e,t,n,r,o)=>(Ai(!0),yp(t[q],r,cf()));function Bl(){return S()}function Xr(e,t,n){let r=S(),o=Gn();if(bn(r,o,t)){let i=ee(),s=Ti();Op(s,r,e,t,r[q],n)}return Xr}var Jr="en-US";var X_=Jr;function wm(e){typeof e=="string"&&(X_=e.toLowerCase().replace(/_/g,"-"))}function Fe(e,t,n){let r=S(),o=ee(),i=be();return J_(o,r,r[q],i,e,t,n),Fe}function J_(e,t,n,r,o,i,s){let a=!0,c=null;if((r.type&3||s)&&(c??=Sc(r,t,i),f_(r,e,t,s,n,o,i,c)&&(a=!1)),a){let l=r.outputs?.[o],d=r.hostDirectiveOutputs?.[o];if(d&&d.length)for(let u=0;u<d.length;u+=2){let p=d[u],f=d[u+1];c??=Sc(r,t,i),Pf(r,t,p,f,o,c)}if(l&&l.length)for(let u of l)c??=Sc(r,t,i),Pf(r,t,u,o,o,c)}}function Ge(e=1){return af(e)}function eD(e,t){let n=null,r=zy(e);for(let o=0;o<t.length;o++){let i=t[o];if(i==="*"){n=o;continue}if(r===null?Ep(e,i,!0):qy(r,i))return o}return n}function yt(e){let t=S()[Ee][ye];if(!t.projection){let n=e?e.length:1,r=t.projection=Fu(n,null),o=r.slice(),i=t.child;for(;i!==null;){if(i.type!==128){let s=e?eD(i,e):0;s!==null&&(o[s]?o[s].projectionNext=i:r[s]=i,o[s]=i)}i=i.next}}}function te(e,t=0,n,r,o,i){let s=S(),a=ee(),c=r?e+1:null;c!==null&&kl(s,a,c,r,o,i,null,n);let l=nr(a,Q+e,16,null,n||null);l.projection===null&&(l.projection=t),gc();let u=!s[jn]||uc();s[Ee][ye].projection[l.projection]===null&&c!==null?tD(s,a,c):u&&!Xi(l)&&mb(a,s,l)}function tD(e,t,n){let r=Q+n,o=t.data[r],i=e[r],s=qc(i,o.tView.ssrId),a=Ml(e,o,void 0,{dehydratedView:s});Sl(i,a,0,zi(o,s))}function ss(e,t,n,r){return dm(e,t,n,r),ss}function eo(e,t,n){return lm(e,t,n),eo}function ge(e){let t=S(),n=ee(),r=Ii();jr(r+1);let o=Rl(n,r);if(e.dirty&&$u(t)===((o.metadata.flags&2)===2)){if(o.matches===null)e.reset([]);else{let i=fm(t,r);e.reset(i,ap),e.notifyOnChanges()}return!0}return!1}function ve(){return Nl(S(),Ii())}function as(e,t,n,r,o){return mm(t,dm(e,n,r,o)),as}function cs(e,t,n,r){return mm(e,lm(t,n,r)),cs}function ls(e=1){jr(Ii()+e)}function ds(e){let t=Ku();return Uu(t,Q+e)}function Ri(e,t){return e<<17|t<<2}function gn(e){return e>>17&32767}function nD(e){return(e&2)==2}function rD(e,t){return e&131071|t<<17}function il(e){return e|2}function Xn(e){return(e&131068)>>2}function Rc(e,t){return e&-131069|t<<2}function oD(e){return(e&1)===1}function sl(e){return e|1}function iD(e,t,n,r,o,i){let s=i?t.classBindings:t.styleBindings,a=gn(s),c=Xn(s);e[r]=n;let l=!1,d;if(Array.isArray(n)){let u=n;d=u[1],(d===null||Pn(u,d)>0)&&(l=!0)}else d=n;if(o)if(c!==0){let p=gn(e[a+1]);e[r+1]=Ri(p,a),p!==0&&(e[p+1]=Rc(e[p+1],r)),e[a+1]=rD(e[a+1],r)}else e[r+1]=Ri(a,0),a!==0&&(e[a+1]=Rc(e[a+1],r)),a=r;else e[r+1]=Ri(c,0),a===0?a=r:e[c+1]=Rc(e[c+1],r),c=r;l&&(e[r+1]=il(e[r+1])),Uf(e,d,r,!0),Uf(e,d,r,!1),sD(t,d,e,r,i),s=Ri(a,c),i?t.classBindings=s:t.styleBindings=s}function sD(e,t,n,r,o){let i=o?e.residualClasses:e.residualStyles;i!=null&&typeof t=="string"&&Pn(i,t)>=0&&(n[r+1]=sl(n[r+1]))}function Uf(e,t,n,r){let o=e[n+1],i=t===null,s=r?gn(o):Xn(o),a=!1;for(;s!==0&&(a===!1||i);){let c=e[s],l=e[s+1];aD(c,t)&&(a=!0,e[s+1]=r?sl(l):il(l)),s=r?gn(l):Xn(l)}a&&(e[n+1]=r?il(o):sl(o))}function aD(e,t){return e===null||t==null||(Array.isArray(e)?e[1]:e)===t?!0:Array.isArray(e)&&typeof t=="string"?Pn(e,t)>=0:!1}var Be={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function cD(e){return e.substring(Be.key,Be.keyEnd)}function lD(e){return dD(e),Im(e,xm(e,0,Be.textEnd))}function Im(e,t){let n=Be.textEnd;return n===t?-1:(t=Be.keyEnd=uD(e,Be.key=t,n),xm(e,t,n))}function dD(e){Be.key=0,Be.keyEnd=0,Be.value=0,Be.valueEnd=0,Be.textEnd=e.length}function xm(e,t,n){for(;t<n&&e.charCodeAt(t)<=32;)t++;return t}function uD(e,t,n){for(;t<n&&e.charCodeAt(t)>32;)t++;return t}function X(e,t){return pD(e,t,null,!0),X}function Hl(e){mD(_D,fD,e,!0)}function fD(e,t){for(let n=lD(t);n>=0;n=Im(t,n))mi(e,cD(t),!0)}function pD(e,t,n,r){let o=S(),i=ee(),s=yc(2);if(i.firstUpdatePass&&Tm(i,e,s,r),t!==$e&&bn(o,s,t)){let a=i.data[Ot()];Sm(i,a,o,o[q],e,o[s+1]=ED(t,n),r,s)}}function mD(e,t,n,r){let o=ee(),i=yc(2);o.firstUpdatePass&&Tm(o,null,i,r);let s=S();if(n!==$e&&bn(s,i,n)){let a=o.data[Ot()];if(Am(a,r)&&!Mm(o,i)){let c=r?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(n=li(c,n||"")),ol(o,a,s,n,r)}else DD(o,a,s,s[q],s[i+1],s[i+1]=bD(e,t,n),r,i)}}function Mm(e,t){return t>=e.expandoStartIndex}function Tm(e,t,n,r){let o=e.data;if(o[n+1]===null){let i=o[Ot()],s=Mm(e,n);Am(i,r)&&t===null&&!s&&(t=!1),t=hD(o,i,t,r),iD(o,i,t,n,s,r)}}function hD(e,t,n,r){let o=nf(e),i=r?t.residualClasses:t.residualStyles;if(o===null)(r?t.classBindings:t.styleBindings)===0&&(n=Fc(null,e,t,n,r),n=Yr(n,t.attrs,r),i=null);else{let s=t.directiveStylingLast;if(s===-1||e[s]!==o)if(n=Fc(o,e,t,n,r),i===null){let c=gD(e,t,r);c!==void 0&&Array.isArray(c)&&(c=Fc(null,e,t,c[1],r),c=Yr(c,t.attrs,r),vD(e,t,r,c))}else i=yD(e,t,r)}return i!==void 0&&(r?t.residualClasses=i:t.residualStyles=i),n}function gD(e,t,n){let r=n?t.classBindings:t.styleBindings;if(Xn(r)!==0)return e[gn(r)]}function vD(e,t,n,r){let o=n?t.classBindings:t.styleBindings;e[gn(o)]=r}function yD(e,t,n){let r,o=t.directiveEnd;for(let i=1+t.directiveStylingLast;i<o;i++){let s=e[i].hostAttrs;r=Yr(r,s,n)}return Yr(r,t.attrs,n)}function Fc(e,t,n,r,o){let i=null,s=n.directiveEnd,a=n.directiveStylingLast;for(a===-1?a=n.directiveStart:a++;a<s&&(i=t[a],r=Yr(r,i.hostAttrs,o),i!==e);)a++;return e!==null&&(n.directiveStylingLast=a),r}function Yr(e,t,n){let r=n?1:2,o=-1;if(t!==null)for(let i=0;i<t.length;i++){let s=t[i];typeof s=="number"?o=s:o===r&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),mi(e,s,n?!0:t[++i]))}return e===void 0?null:e}function bD(e,t,n){if(n==null||n==="")return me;let r=[],o=fl(n);if(Array.isArray(o))for(let i=0;i<o.length;i++)e(r,o[i],!0);else if(o instanceof Set)for(let i of o)e(r,i,!0);else if(typeof o=="object")for(let i in o)o.hasOwnProperty(i)&&e(r,i,o[i]);else typeof o=="string"&&t(r,o);return r}function _D(e,t,n){let r=String(t);r!==""&&!r.includes(" ")&&mi(e,r,n)}function DD(e,t,n,r,o,i,s,a){o===$e&&(o=me);let c=0,l=0,d=0<o.length?o[0]:null,u=0<i.length?i[0]:null;for(;d!==null||u!==null;){let p=c<o.length?o[c+1]:void 0,f=l<i.length?i[l+1]:void 0,h=null,C;d===u?(c+=2,l+=2,p!==f&&(h=u,C=f)):u===null||d!==null&&d<u?(c+=2,h=d):(l+=2,h=u,C=f),h!==null&&Sm(e,t,n,r,h,C,s,a),d=c<o.length?o[c]:null,u=l<i.length?i[l]:null}}function Sm(e,t,n,r,o,i,s,a){if(!(t.type&3))return;let c=e.data,l=c[a+1],d=oD(l)?$f(c,t,n,o,Xn(l),s):void 0;if(!Yi(d)){Yi(i)||nD(l)&&(i=$f(c,null,n,o,a,s));let u=rc(Ot(),n);gb(r,s,u,o,i)}}function $f(e,t,n,r,o,i){let s=t===null,a;for(;o>0;){let c=e[o],l=Array.isArray(c),d=l?c[1]:c,u=d===null,p=n[o+1];p===$e&&(p=u?me:void 0);let f=u?hi(p,r):d===r?p:void 0;if(l&&!Yi(f)&&(f=hi(c,r)),Yi(f)&&(a=f,s))return a;let h=e[o+1];o=s?gn(h):Xn(h)}if(t!==null){let c=i?t.residualClasses:t.residualStyles;c!=null&&(a=hi(c,r))}return a}function Yi(e){return e!==void 0}function ED(e,t){return e==null||e===""||(typeof t=="string"?e=e+t:typeof e=="object"&&(e=ci(fl(e)))),e}function Am(e,t){return(e.flags&(t?8:16))!==0}function xe(e,t=""){let n=S(),r=ee(),o=e+Q,i=r.firstCreatePass?nr(r,o,1,t,null):r.data[o],s=CD(r,n,i,t);n[o]=s,Si()&&El(r,n,s,i),zn(i,!1)}var CD=(e,t,n,r)=>(Ai(!0),Oy(t[q],r));function wD(e,t,n,r=""){return bn(e,Gn(),n)?t+Ga(n)+r:$e}function or(e){return ir("",e),or}function ir(e,t,n){let r=S(),o=wD(r,e,t,n);return o!==$e&&ID(r,Ot(),o),ir}function ID(e,t,n){let r=rc(t,e);ky(e[q],r,n)}function zf(e,t,n){let r=ee();r.firstCreatePass&&Nm(t,r.data,r.blueprint,et(e),n)}function Nm(e,t,n,r,o){if(e=ae(e),Array.isArray(e))for(let i=0;i<e.length;i++)Nm(e[i],t,n,r,o);else{let i=ee(),s=S(),a=be(),c=nn(e)?e:ae(e.provide),l=Ja(e),d=a.providerIndexes&1048575,u=a.directiveStart,p=a.providerIndexes>>20;if(nn(e)||!e.multi){let f=new mn(l,o,j,null),h=kc(c,t,o?d:d+p,u);h===-1?(Lc(Bi(a,s),i,c),Oc(i,e,t.length),t.push(c),a.directiveStart++,a.directiveEnd++,o&&(a.providerIndexes+=1048576),n.push(f),s.push(f)):(n[h]=f,s[h]=f)}else{let f=kc(c,t,d+p,u),h=kc(c,t,d,d+p),C=f>=0&&n[f],y=h>=0&&n[h];if(o&&!y||!o&&!C){Lc(Bi(a,s),i,c);let b=TD(o?MD:xD,n.length,o,r,l,e);!o&&y&&(n[h].providerFactory=b),Oc(i,e,t.length,0),t.push(c),a.directiveStart++,a.directiveEnd++,o&&(a.providerIndexes+=1048576),n.push(b),s.push(b)}else{let b=Rm(n[o?h:f],l,!o&&r);Oc(i,e,f>-1?f:h,b)}!o&&r&&y&&n[h].componentProviders++}}}function Oc(e,t,n,r){let o=nn(t),i=Lu(t);if(o||i){let c=(i?ae(t.useClass):t).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!o&&t.multi){let d=l.indexOf(n);d===-1?l.push(n,[r,c]):l[d+1].push(r,c)}else l.push(n,c)}}}function Rm(e,t,n){return n&&e.componentProviders++,e.multi.push(t)-1}function kc(e,t,n,r){for(let o=n;o<r;o++)if(t[o]===e)return o;return-1}function xD(e,t,n,r,o){return al(this.multi,[])}function MD(e,t,n,r,o){let i=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Gr(r,r[D],this.providerFactory.index,o);s=c.slice(0,a),al(i,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],al(i,s);return s}function al(e,t){for(let n=0;n<e.length;n++){let r=e[n];t.push(r())}return t}function TD(e,t,n,r,o,i){let s=new mn(e,n,j,null);return s.multi=[],s.index=t,s.componentProviders=0,Rm(s,o,r&&!n),s}function bt(e,t){return n=>{n.providersResolver=(r,o)=>zf(r,o?o(e):e,!1),t&&(n.viewProvidersResolver=(r,o)=>zf(r,o?o(t):t,!0))}}function Ul(e,t){return rs(e,t)}var Fm=(()=>{class e{applicationErrorHandler=m(Lt);appRef=m(Dn);taskService=m(fn);ngZone=m(N);zonelessEnabled=m(Hr);tracing=m(ot,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new se;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ar):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(m(wc,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let n=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(n);return}this.switchToMicrotaskScheduler(),this.taskService.remove(n)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let n=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(n)})})}notify(n){if(!this.zonelessEnabled&&n===5)return;switch(n){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?pf:Dc;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ar+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let n=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(n),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let n=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(n)}}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Om(){return[{provide:Ke,useExisting:Fm},{provide:N,useClass:Nr},{provide:Hr,useValue:!0}]}function SD(){return typeof $localize<"u"&&$localize.locale||Jr}var us=new g("",{factory:()=>m(us,{optional:!0,skipSelf:!0})||SD()});function st(e){return Du(e)}function at(e,t){return _r(e,t?.equal)}function km(e,t){return Fl(t)}function GD(e,t){return Ol(t)}var no=(km.required=GD,km);function Pm(e,t){return Fl(t)}function WD(e,t){return Ol(t)}var Bm=(Pm.required=WD,Pm);var zl=new g(""),qD=new g("");function to(e){return!e.moduleRef}function ZD(e){let t=to(e)?e.r3Injector:e.moduleRef.injector,n=t.get(N);return n.run(()=>{to(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let r=t.get(Lt),o;if(n.runOutsideAngular(()=>{o=n.onError.subscribe({next:r})}),to(e)){let i=()=>t.destroy(),s=e.platformInjector.get(zl);s.add(i),t.onDestroy(()=>{o.unsubscribe(),s.delete(i)})}else{let i=()=>e.moduleRef.destroy(),s=e.platformInjector.get(zl);s.add(i),e.moduleRef.onDestroy(()=>{zr(e.allPlatformModules,e.moduleRef),o.unsubscribe(),s.delete(i)})}return QD(r,n,()=>{let i=t.get(fn),s=i.add(),a=t.get(Vl);return a.runInitializers(),a.donePromise.then(()=>{let c=t.get(us,Jr);if(wm(c||Jr),!t.get(qD,!0))return to(e)?t.get(Dn):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(to(e)){let d=t.get(Dn);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return YD?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{i.remove(s)})})})}var YD;function QD(e,t,n){try{let r=n();return rr(r)?r.catch(o=>{throw t.runOutsideAngular(()=>e(o)),o}):r}catch(r){throw t.runOutsideAngular(()=>e(r)),r}}var fs=null;function KD(e=[],t){return de.create({name:t,providers:[{provide:kr,useValue:"platform"},{provide:zl,useValue:new Set([()=>fs=null])},...e]})}function XD(e=[]){if(fs)return fs;let t=KD(e);return fs=t,Dm(),JD(t),t}function JD(e){let t=e.get(Ki,null);Vn(e,()=>{t?.forEach(n=>n())})}var eE=1e4;var HP=eE-1e3;var sr=(()=>{class e{static __NG_ELEMENT_ID__=tE}return e})();function tE(e){return nE(be(),S(),(e&16)===16)}function nE(e,t,n){if(ht(e)&&!n){let r=Ae(e.index,t);return new Vt(r,r)}else if(e.type&175){let r=t[Ee];return new Vt(r,t)}return null}var Gl=class{supports(t){return Al(t)}create(t){return new Wl(t)}},rE=(e,t)=>t,Wl=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(t){this._trackByFn=t||rE}forEachItem(t){let n;for(n=this._itHead;n!==null;n=n._next)t(n)}forEachOperation(t){let n=this._itHead,r=this._removalsHead,o=0,i=null;for(;n||r;){let s=!r||n&&n.currentIndex<Lm(r,o,i)?n:r,a=Lm(s,o,i),c=s.currentIndex;if(s===r)o--,r=r._nextRemoved;else if(n=n._next,s.previousIndex==null)o++;else{i||(i=[]);let l=a-o,d=c-o;if(l!=d){for(let p=0;p<l;p++){let f=p<i.length?i[p]:i[p]=0,h=f+p;d<=h&&h<l&&(i[p]=f+1)}let u=s.previousIndex;i[u]=d-l}}a!==c&&t(s,a,c)}}forEachPreviousItem(t){let n;for(n=this._previousItHead;n!==null;n=n._nextPrevious)t(n)}forEachAddedItem(t){let n;for(n=this._additionsHead;n!==null;n=n._nextAdded)t(n)}forEachMovedItem(t){let n;for(n=this._movesHead;n!==null;n=n._nextMoved)t(n)}forEachRemovedItem(t){let n;for(n=this._removalsHead;n!==null;n=n._nextRemoved)t(n)}forEachIdentityChange(t){let n;for(n=this._identityChangesHead;n!==null;n=n._nextIdentityChange)t(n)}diff(t){if(t==null&&(t=[]),!Al(t))throw new _(900,!1);return this.check(t)?this:null}onDestroy(){}check(t){this._reset();let n=this._itHead,r=!1,o,i,s;if(Array.isArray(t)){this.length=t.length;for(let a=0;a<this.length;a++)i=t[a],s=this._trackByFn(a,i),n===null||!Object.is(n.trackById,s)?(n=this._mismatch(n,i,s,a),r=!0):(r&&(n=this._verifyReinsertion(n,i,s,a)),Object.is(n.item,i)||this._addIdentityChange(n,i)),n=n._next}else o=0,tm(t,a=>{s=this._trackByFn(o,a),n===null||!Object.is(n.trackById,s)?(n=this._mismatch(n,a,s,o),r=!0):(r&&(n=this._verifyReinsertion(n,a,s,o)),Object.is(n.item,a)||this._addIdentityChange(n,a)),n=n._next,o++}),this.length=o;return this._truncate(n),this.collection=t,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let t;for(t=this._previousItHead=this._itHead;t!==null;t=t._next)t._nextPrevious=t._next;for(t=this._additionsHead;t!==null;t=t._nextAdded)t.previousIndex=t.currentIndex;for(this._additionsHead=this._additionsTail=null,t=this._movesHead;t!==null;t=t._nextMoved)t.previousIndex=t.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(t,n,r,o){let i;return t===null?i=this._itTail:(i=t._prev,this._remove(t)),t=this._unlinkedRecords===null?null:this._unlinkedRecords.get(r,null),t!==null?(Object.is(t.item,n)||this._addIdentityChange(t,n),this._reinsertAfter(t,i,o)):(t=this._linkedRecords===null?null:this._linkedRecords.get(r,o),t!==null?(Object.is(t.item,n)||this._addIdentityChange(t,n),this._moveAfter(t,i,o)):t=this._addAfter(new ql(n,r),i,o)),t}_verifyReinsertion(t,n,r,o){let i=this._unlinkedRecords===null?null:this._unlinkedRecords.get(r,null);return i!==null?t=this._reinsertAfter(i,t._prev,o):t.currentIndex!=o&&(t.currentIndex=o,this._addToMoves(t,o)),t}_truncate(t){for(;t!==null;){let n=t._next;this._addToRemovals(this._unlink(t)),t=n}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(t,n,r){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(t);let o=t._prevRemoved,i=t._nextRemoved;return o===null?this._removalsHead=i:o._nextRemoved=i,i===null?this._removalsTail=o:i._prevRemoved=o,this._insertAfter(t,n,r),this._addToMoves(t,r),t}_moveAfter(t,n,r){return this._unlink(t),this._insertAfter(t,n,r),this._addToMoves(t,r),t}_addAfter(t,n,r){return this._insertAfter(t,n,r),this._additionsTail===null?this._additionsTail=this._additionsHead=t:this._additionsTail=this._additionsTail._nextAdded=t,t}_insertAfter(t,n,r){let o=n===null?this._itHead:n._next;return t._next=o,t._prev=n,o===null?this._itTail=t:o._prev=t,n===null?this._itHead=t:n._next=t,this._linkedRecords===null&&(this._linkedRecords=new ps),this._linkedRecords.put(t),t.currentIndex=r,t}_remove(t){return this._addToRemovals(this._unlink(t))}_unlink(t){this._linkedRecords!==null&&this._linkedRecords.remove(t);let n=t._prev,r=t._next;return n===null?this._itHead=r:n._next=r,r===null?this._itTail=n:r._prev=n,t}_addToMoves(t,n){return t.previousIndex===n||(this._movesTail===null?this._movesTail=this._movesHead=t:this._movesTail=this._movesTail._nextMoved=t),t}_addToRemovals(t){return this._unlinkedRecords===null&&(this._unlinkedRecords=new ps),this._unlinkedRecords.put(t),t.currentIndex=null,t._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=t,t._prevRemoved=null):(t._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=t),t}_addIdentityChange(t,n){return t.item=n,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=t:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=t,t}},ql=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(t,n){this.item=t,this.trackById=n}},Zl=class{_head=null;_tail=null;add(t){this._head===null?(this._head=this._tail=t,t._nextDup=null,t._prevDup=null):(this._tail._nextDup=t,t._prevDup=this._tail,t._nextDup=null,this._tail=t)}get(t,n){let r;for(r=this._head;r!==null;r=r._nextDup)if((n===null||n<=r.currentIndex)&&Object.is(r.trackById,t))return r;return null}remove(t){let n=t._prevDup,r=t._nextDup;return n===null?this._head=r:n._nextDup=r,r===null?this._tail=n:r._prevDup=n,this._head===null}},ps=class{map=new Map;put(t){let n=t.trackById,r=this.map.get(n);r||(r=new Zl,this.map.set(n,r)),r.add(t)}get(t,n){let r=t,o=this.map.get(r);return o?o.get(t,n):null}remove(t){let n=t.trackById;return this.map.get(n).remove(t)&&this.map.delete(n),t}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function Lm(e,t,n){let r=e.previousIndex;if(r===null)return r;let o=0;return n&&r<n.length&&(o=n[r]),r+t+o}function Vm(){return new Ql([new Gl])}var Ql=(()=>{class e{factories;static \u0275prov=v({token:e,providedIn:"root",factory:Vm});constructor(n){this.factories=n}static create(n,r){if(r!=null){let o=r.factories.slice();n=n.concat(o)}return new e(n)}static extend(n){return{provide:e,useFactory:()=>{let r=m(e,{optional:!0,skipSelf:!0});return e.create(n,r||Vm())}}}find(n){let r=this.factories.find(o=>o.supports(n));if(r!=null)return r;throw new _(901,!1)}}return e})();function Hm(e){let{rootComponent:t,appProviders:n,platformProviders:r,platformRef:o}=e;R(A.BootstrapApplicationStart);try{let i=o?.injector??XD(r),s=[Om(),hf,...n||[]],a=new Zr({providers:s,parent:i,debugName:"",runEnvironmentInitializers:!1});return ZD({r3Injector:a.injector,platformInjector:i,rootComponent:t})}catch(i){return Promise.reject(i)}finally{R(A.BootstrapApplicationEnd)}}function ar(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Um(e,t=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):t}var $l=Symbol("NOT_SET"),$m=new Set,oE=P(I({},Fo),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:$l,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==$l&&!xn(this))return this.signal;try{for(let o of this.cleanup??$m)o()}finally{this.cleanup?.clear()}let t=[];e!==void 0&&t.push(e),t.push(this.registerCleanupFn);let n=Dt(this),r;try{r=this.userFn.apply(null,t)}finally{qt(this,n)}return(this.value===$l||!this.equal(this.value,r))&&(this.value=r,this.version++),this.signal}}),Yl=class extends $i{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(t,n,r,o,i,s=null){super(t,[void 0,void 0,void 0,void 0],r,!1,i.get(Pt),s),this.scheduler=o;for(let a of bl){let c=n[a];if(c===void 0)continue;let l=Object.create(oE);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(In(l),l.value),l.signal[ie]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();super.destroy();for(let t of this.nodes)if(t)try{for(let n of t.cleanup??$m)n()}finally{Et(t)}}};function zm(e,t){let n=t?.injector??m(de),r=n.get(Ke),o=n.get(yl),i=n.get(ot,null,{optional:!0});o.impl??=n.get(xp);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=n.get(Br,null,{optional:!0}),c=new Yl(o.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,r,n,i?.snapshot(null));return o.impl.register(c),c}function Gm(e,t){let n=Tt(e),r=t.elementInjector||Ln();return new Kn(n).create(r,t.projectableNodes,t.hostElement,t.environmentInjector,t.directives,t.bindings)}var Wm=null;function We(){return Wm}function Kl(e){Wm??=e}var ro=class{},ms=(()=>{class e{historyGo(n){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:()=>m(qm),providedIn:"platform"})}return e})();var qm=(()=>{class e extends ms{_location;_history;_doc=m(Z);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return We().getBaseHref(this._doc)}onPopState(n){let r=We().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",n,!1),()=>r.removeEventListener("popstate",n)}onHashChange(n){let r=We().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",n,!1),()=>r.removeEventListener("hashchange",n)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(n){this._location.pathname=n}pushState(n,r,o){this._history.pushState(n,r,o)}replaceState(n,r,o){this._history.replaceState(n,r,o)}forward(){this._history.forward()}back(){this._history.back()}historyGo(n=0){this._history.go(n)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:()=>new e,providedIn:"platform"})}return e})();var hs=class{$implicit;ngForOf;index;count;constructor(t,n,r,o){this.$implicit=t,this.ngForOf=n,this.index=r,this.count=o}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},gs=(()=>{class e{_viewContainer;_template;_differs;set ngForOf(n){this._ngForOf=n,this._ngForOfDirty=!0}set ngForTrackBy(n){this._trackByFn=n}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(n,r,o){this._viewContainer=n,this._template=r,this._differs=o}set ngForTemplate(n){n&&(this._template=n)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let n=this._ngForOf;!this._differ&&n&&(this._differ=this._differs.find(n).create(this.ngForTrackBy))}if(this._differ){let n=this._differ.diff(this._ngForOf);n&&this._applyChanges(n)}}_applyChanges(n){let r=this._viewContainer;n.forEachOperation((o,i,s)=>{if(o.previousIndex==null)r.createEmbeddedView(this._template,new hs(o.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)r.remove(i===null?void 0:i);else if(i!==null){let a=r.get(i);r.move(a,s),Zm(a,o)}});for(let o=0,i=r.length;o<i;o++){let a=r.get(o).context;a.index=o,a.count=i,a.ngForOf=this._ngForOf}n.forEachIdentityChange(o=>{let i=r.get(o.currentIndex);Zm(i,o)})}static ngTemplateContextGuard(n,r){return!0}static \u0275fac=function(r){return new(r||e)(j(_n),j(hn),j(Ql))};static \u0275dir=O({type:e,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return e})();function Zm(e,t){e.context.$implicit=t.item}var Xl=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=m(de);constructor(n){this._viewContainerRef=n}ngOnChanges(n){if(this._shouldRecreateView(n)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let o=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,o,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(n){return!!n.ngTemplateOutlet||!!n.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(n,r,o)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,o):!1,get:(n,r,o)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,o)}})}static \u0275fac=function(r){return new(r||e)(j(_n))};static \u0275dir=O({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[vn]})}return e})();var vs=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();function oo(e,t){t=encodeURIComponent(t);for(let n of e.split(";")){let r=n.indexOf("="),[o,i]=r==-1?[n,""]:[n.slice(0,r),n.slice(r+1)];if(o.trim()===t)return decodeURIComponent(i)}return null}var En=class{};var Jl="browser";function Ym(e){return e===Jl}var io=class{_doc;constructor(t){this._doc=t}manager},ys=(()=>{class e extends io{constructor(n){super(n)}supports(n){return!0}addEventListener(n,r,o,i){return n.addEventListener(r,o,i),()=>this.removeEventListener(n,r,o,i)}removeEventListener(n,r,o,i){return n.removeEventListener(r,o,i)}static \u0275fac=function(r){return new(r||e)(M(Z))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),Ds=new g(""),rd=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(n,r){this._zone=r,n.forEach(s=>{s.manager=this});let o=n.filter(s=>!(s instanceof ys));this._plugins=o.slice().reverse();let i=n.find(s=>s instanceof ys);i&&this._plugins.push(i)}addEventListener(n,r,o,i){return this._findPluginFor(r).addEventListener(n,r,o,i)}getZone(){return this._zone}_findPluginFor(n){let r=this._eventNameToPlugin.get(n);if(r)return r;if(r=this._plugins.find(i=>i.supports(n)),!r)throw new _(5101,!1);return this._eventNameToPlugin.set(n,r),r}static \u0275fac=function(r){return new(r||e)(M(Ds),M(N))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),ed="ng-app-id";function Qm(e){for(let t of e)t.remove()}function Km(e,t){let n=t.createElement("style");return n.textContent=e,n}function sE(e,t,n,r){let o=e.head?.querySelectorAll(`style[${ed}="${t}"],link[${ed}="${t}"]`);if(o)for(let i of o)i.removeAttribute(ed),i instanceof HTMLLinkElement?r.set(i.href.slice(i.href.lastIndexOf("/")+1),{usage:0,elements:[i]}):i.textContent&&n.set(i.textContent,{usage:0,elements:[i]})}function nd(e,t){let n=t.createElement("link");return n.setAttribute("rel","stylesheet"),n.setAttribute("href",e),n}var od=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(n,r,o,i={}){this.doc=n,this.appId=r,this.nonce=o,sE(n,r,this.inline,this.external),this.hosts.add(n.head)}addStyles(n,r){for(let o of n)this.addUsage(o,this.inline,Km);r?.forEach(o=>this.addUsage(o,this.external,nd))}removeStyles(n,r){for(let o of n)this.removeUsage(o,this.inline);r?.forEach(o=>this.removeUsage(o,this.external))}addUsage(n,r,o){let i=r.get(n);i?i.usage++:r.set(n,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,o(n,this.doc)))})}removeUsage(n,r){let o=r.get(n);o&&(o.usage--,o.usage<=0&&(Qm(o.elements),r.delete(n)))}ngOnDestroy(){for(let[,{elements:n}]of[...this.inline,...this.external])Qm(n);this.hosts.clear()}addHost(n){this.hosts.add(n);for(let[r,{elements:o}]of this.inline)o.push(this.addElement(n,Km(r,this.doc)));for(let[r,{elements:o}]of this.external)o.push(this.addElement(n,nd(r,this.doc)))}removeHost(n){this.hosts.delete(n)}addElement(n,r){return this.nonce&&r.setAttribute("nonce",this.nonce),n.appendChild(r)}static \u0275fac=function(r){return new(r||e)(M(Z),M(er),M(tr,8),M(yn))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),td={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},id=/%COMP%/g;var Jm="%COMP%",aE=`_nghost-${Jm}`,cE=`_ngcontent-${Jm}`,lE=!0,dE=new g("",{factory:()=>lE});function uE(e){return cE.replace(id,e)}function fE(e){return aE.replace(id,e)}function eh(e,t){return t.map(n=>n.replace(id,e))}var sd=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(n,r,o,i,s,a,c=null,l=null){this.eventManager=n,this.sharedStylesHost=r,this.appId=o,this.removeStylesOnCompDestroy=i,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new so(n,s,a,this.tracingService)}createRenderer(n,r){if(!n||!r)return this.defaultRenderer;let o=this.getOrCreateRenderer(n,r);return o instanceof _s?o.applyToHost(n):o instanceof ao&&o.applyStyles(),o}getOrCreateRenderer(n,r){let o=this.rendererByCompId,i=o.get(r.id);if(!i){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(r.encapsulation){case He.Emulated:i=new _s(c,l,r,this.appId,d,s,a,u);break;case He.ShadowDom:return new bs(c,n,r,s,a,this.nonce,u,l);case He.ExperimentalIsolatedShadowDom:return new bs(c,n,r,s,a,this.nonce,u);default:i=new ao(c,l,r,d,s,a,u);break}o.set(r.id,i)}return i}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(n){this.rendererByCompId.delete(n)}static \u0275fac=function(r){return new(r||e)(M(rd),M(od),M(er),M(dE),M(Z),M(N),M(tr),M(ot,8))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),so=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(t,n,r,o){this.eventManager=t,this.doc=n,this.ngZone=r,this.tracingService=o}destroy(){}destroyNode=null;createElement(t,n){return n?this.doc.createElementNS(td[n]||n,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,n){(Xm(t)?t.content:t).appendChild(n)}insertBefore(t,n,r){t&&(Xm(t)?t.content:t).insertBefore(n,r)}removeChild(t,n){n.remove()}selectRootElement(t,n){let r=typeof t=="string"?this.doc.querySelector(t):t;if(!r)throw new _(-5104,!1);return n||(r.textContent=""),r}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,n,r,o){if(o){n=o+":"+n;let i=td[o];i?t.setAttributeNS(i,n,r):t.setAttribute(n,r)}else t.setAttribute(n,r)}removeAttribute(t,n,r){if(r){let o=td[r];o?t.removeAttributeNS(o,n):t.removeAttribute(`${r}:${n}`)}else t.removeAttribute(n)}addClass(t,n){t.classList.add(n)}removeClass(t,n){t.classList.remove(n)}setStyle(t,n,r,o){o&(rt.DashCase|rt.Important)?t.style.setProperty(n,r,o&rt.Important?"important":""):t.style[n]=r}removeStyle(t,n,r){r&rt.DashCase?t.style.removeProperty(n):t.style[n]=""}setProperty(t,n,r){t!=null&&(t[n]=r)}setValue(t,n){t.nodeValue=n}listen(t,n,r,o){if(typeof t=="string"&&(t=We().getGlobalEventTarget(this.doc,t),!t))throw new _(5102,!1);let i=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(i=this.tracingService.wrapEventListener(t,n,i)),this.eventManager.addEventListener(t,n,i,o)}decoratePreventDefault(t){return n=>{if(n==="__ngUnwrap__")return t;t(n)===!1&&n.preventDefault()}}};function Xm(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var bs=class extends so{hostEl;sharedStylesHost;shadowRoot;constructor(t,n,r,o,i,s,a,c){super(t,o,i,a),this.hostEl=n,this.sharedStylesHost=c,this.shadowRoot=n.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=r.styles;l=eh(r.id,l);for(let u of l){let p=document.createElement("style");s&&p.setAttribute("nonce",s),p.textContent=u,this.shadowRoot.appendChild(p)}let d=r.getExternalStyles?.();if(d)for(let u of d){let p=nd(u,o);s&&p.setAttribute("nonce",s),this.shadowRoot.appendChild(p)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,n){return super.appendChild(this.nodeOrShadowRoot(t),n)}insertBefore(t,n,r){return super.insertBefore(this.nodeOrShadowRoot(t),n,r)}removeChild(t,n){return super.removeChild(null,n)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ao=class extends so{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(t,n,r,o,i,s,a,c){super(t,i,s,a),this.sharedStylesHost=n,this.removeStylesOnCompDestroy=o;let l=r.styles;this.styles=c?eh(c,l):l,this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Qn.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},_s=class extends ao{contentAttr;hostAttr;constructor(t,n,r,o,i,s,a,c){let l=o+"-"+r.id;super(t,n,r,i,s,a,c,l),this.contentAttr=uE(l),this.hostAttr=fE(l)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,n){let r=super.createElement(t,n);return super.setAttribute(r,this.contentAttr,""),r}};var Es=class e extends ro{supportsDOMEvents=!0;static makeCurrent(){Kl(new e)}onAndCancel(t,n,r,o){return t.addEventListener(n,r,o),()=>{t.removeEventListener(n,r,o)}}dispatchEvent(t,n){t.dispatchEvent(n)}remove(t){t.remove()}createElement(t,n){return n=n||this.getDefaultDocument(),n.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,n){return n==="window"?window:n==="document"?t:n==="body"?t.body:null}getBaseHref(t){let n=pE();return n==null?null:mE(n)}resetBaseElement(){co=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return oo(document.cookie,t)}},co=null;function pE(){return co=co||document.head.querySelector("base"),co?co.getAttribute("href"):null}function mE(e){return new URL(e,document.baseURI).pathname}var hE=(()=>{class e{build(){return new XMLHttpRequest}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),th=["alt","control","meta","shift"],gE={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},vE={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},nh=(()=>{class e extends io{constructor(n){super(n)}supports(n){return e.parseEventName(n)!=null}addEventListener(n,r,o,i){let s=e.parseEventName(r),a=e.eventCallback(s.fullKey,o,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>We().onAndCancel(n,s.domEventName,a,i))}static parseEventName(n){let r=n.toLowerCase().split("."),o=r.shift();if(r.length===0||!(o==="keydown"||o==="keyup"))return null;let i=e._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),th.forEach(l=>{let d=r.indexOf(l);d>-1&&(r.splice(d,1),s+=l+".")}),s+=i,r.length!=0||i.length===0)return null;let c={};return c.domEventName=o,c.fullKey=s,c}static matchEventFullKeyCode(n,r){let o=gE[n.key]||n.key,i="";return r.indexOf("code.")>-1&&(o=n.code,i="code."),o==null||!o?!1:(o=o.toLowerCase(),o===" "?o="space":o==="."&&(o="dot"),th.forEach(s=>{if(s!==o){let a=vE[s];a(n)&&(i+=s+".")}}),i+=o,i===r)}static eventCallback(n,r,o){return i=>{e.matchEventFullKeyCode(i,n)&&o.runGuarded(()=>r(i))}}static _normalizeKey(n){return n==="esc"?"escape":n}static \u0275fac=function(r){return new(r||e)(M(Z))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})();async function ad(e,t,n){let r=I({rootComponent:e},yE(t,n));return Hm(r)}function yE(e,t){return{platformRef:t?.platformRef,appProviders:[...CE,...e?.providers??[]],platformProviders:EE}}function bE(){Es.makeCurrent()}function _E(){return new Qe}function DE(){return dl(document),document}var EE=[{provide:yn,useValue:Jl},{provide:Ki,useValue:bE,multi:!0},{provide:Z,useFactory:DE}];var CE=[{provide:kr,useValue:"root"},{provide:Qe,useFactory:_E},{provide:Ds,useClass:ys,multi:!0},{provide:Ds,useClass:nh,multi:!0},sd,od,rd,{provide:Ue,useExisting:sd},{provide:En,useClass:hE},[]];var Ut=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(t){t?typeof t=="string"?this.lazyInit=()=>{this.headers=new Map,t.split(`
`).forEach(n=>{let r=n.indexOf(":");if(r>0){let o=n.slice(0,r),i=n.slice(r+1).trim();this.addHeaderEntry(o,i)}})}:typeof Headers<"u"&&t instanceof Headers?(this.headers=new Map,t.forEach((n,r)=>{this.addHeaderEntry(r,n)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(t).forEach(([n,r])=>{this.setHeaderEntries(n,r)})}:this.headers=new Map}has(t){return this.init(),this.headers.has(t.toLowerCase())}get(t){this.init();let n=this.headers.get(t.toLowerCase());return n&&n.length>0?n[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(t){return this.init(),this.headers.get(t.toLowerCase())||null}append(t,n){return this.clone({name:t,value:n,op:"a"})}set(t,n){return this.clone({name:t,value:n,op:"s"})}delete(t,n){return this.clone({name:t,value:n,op:"d"})}maybeSetNormalizedName(t,n){this.normalizedNames.has(n)||this.normalizedNames.set(n,t)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(t=>this.applyUpdate(t)),this.lazyUpdate=null))}copyFrom(t){t.init(),Array.from(t.headers.keys()).forEach(n=>{this.headers.set(n,t.headers.get(n)),this.normalizedNames.set(n,t.normalizedNames.get(n))})}clone(t){let n=new e;return n.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,n.lazyUpdate=(this.lazyUpdate||[]).concat([t]),n}applyUpdate(t){let n=t.name.toLowerCase();switch(t.op){case"a":case"s":let r=t.value;if(typeof r=="string"&&(r=[r]),r.length===0)return;this.maybeSetNormalizedName(t.name,n);let o=(t.op==="a"?this.headers.get(n):void 0)||[];o.push(...r),this.headers.set(n,o);break;case"d":let i=t.value;if(!i)this.headers.delete(n),this.normalizedNames.delete(n);else{let s=this.headers.get(n);if(!s)return;s=s.filter(a=>i.indexOf(a)===-1),s.length===0?(this.headers.delete(n),this.normalizedNames.delete(n)):this.headers.set(n,s)}break}}addHeaderEntry(t,n){let r=t.toLowerCase();this.maybeSetNormalizedName(t,r),this.headers.has(r)?this.headers.get(r).push(n):this.headers.set(r,[n])}setHeaderEntries(t,n){let r=(Array.isArray(n)?n:[n]).map(i=>i.toString()),o=t.toLowerCase();this.headers.set(o,r),this.maybeSetNormalizedName(t,o)}forEach(t){this.init(),Array.from(this.normalizedNames.keys()).forEach(n=>t(this.normalizedNames.get(n),this.headers.get(n)))}};var ws=class{map=new Map;set(t,n){return this.map.set(t,n),this}get(t){return this.map.has(t)||this.map.set(t,t.defaultValue()),this.map.get(t)}delete(t){return this.map.delete(t),this}has(t){return this.map.has(t)}keys(){return this.map.keys()}},Is=class{encodeKey(t){return rh(t)}encodeValue(t){return rh(t)}decodeKey(t){return decodeURIComponent(t)}decodeValue(t){return decodeURIComponent(t)}};function wE(e,t){let n=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(o=>{let i=o.indexOf("="),[s,a]=i==-1?[t.decodeKey(o),""]:[t.decodeKey(o.slice(0,i)),t.decodeValue(o.slice(i+1))],c=n.get(s)||[];c.push(a),n.set(s,c)}),n}var IE=/%(\d[a-f0-9])/gi,xE={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function rh(e){return encodeURIComponent(e).replace(IE,(t,n)=>xE[n]??t)}function Cs(e){return`${e}`}var _t=class e{map;encoder;updates=null;cloneFrom=null;constructor(t={}){if(this.encoder=t.encoder||new Is,t.fromString){if(t.fromObject)throw new _(2805,!1);this.map=wE(t.fromString,this.encoder)}else t.fromObject?(this.map=new Map,Object.keys(t.fromObject).forEach(n=>{let r=t.fromObject[n],o=Array.isArray(r)?r.map(Cs):[Cs(r)];this.map.set(n,o)})):this.map=null}has(t){return this.init(),this.map.has(t)}get(t){this.init();let n=this.map.get(t);return n?n[0]:null}getAll(t){return this.init(),this.map.get(t)||null}keys(){return this.init(),Array.from(this.map.keys())}append(t,n){return this.clone({param:t,value:n,op:"a"})}appendAll(t){let n=[];return Object.keys(t).forEach(r=>{let o=t[r];Array.isArray(o)?o.forEach(i=>{n.push({param:r,value:i,op:"a"})}):n.push({param:r,value:o,op:"a"})}),this.clone(n)}set(t,n){return this.clone({param:t,value:n,op:"s"})}delete(t,n){return this.clone({param:t,value:n,op:"d"})}toString(){return this.init(),this.keys().map(t=>{let n=this.encoder.encodeKey(t);return this.map.get(t).map(r=>n+"="+this.encoder.encodeValue(r)).join("&")}).filter(t=>t!=="").join("&")}clone(t){let n=new e({encoder:this.encoder});return n.cloneFrom=this.cloneFrom||this,n.updates=(this.updates||[]).concat(t),n}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(t=>this.map.set(t,this.cloneFrom.map.get(t))),this.updates.forEach(t=>{switch(t.op){case"a":case"s":let n=(t.op==="a"?this.map.get(t.param):void 0)||[];n.push(Cs(t.value)),this.map.set(t.param,n);break;case"d":if(t.value!==void 0){let r=this.map.get(t.param)||[],o=r.indexOf(Cs(t.value));o!==-1&&r.splice(o,1),r.length>0?this.map.set(t.param,r):this.map.delete(t.param)}else{this.map.delete(t.param);break}}}),this.cloneFrom=this.updates=null)}};function ME(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function oh(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function ih(e){return typeof Blob<"u"&&e instanceof Blob}function sh(e){return typeof FormData<"u"&&e instanceof FormData}function TE(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var ah="Content-Type",ch="Accept",dh="text/plain",uh="application/json",SE=`${uh}, ${dh}, */*`,cr=class e{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(t,n,r,o){this.url=n,this.method=t.toUpperCase();let i;if(ME(this.method)||o?(this.body=r!==void 0?r:null,i=o):i=r,i){if(this.reportProgress=!!i.reportProgress,this.withCredentials=!!i.withCredentials,this.keepalive=!!i.keepalive,i.responseType&&(this.responseType=i.responseType),i.headers&&(this.headers=i.headers),i.context&&(this.context=i.context),i.params&&(this.params=i.params),i.priority&&(this.priority=i.priority),i.cache&&(this.cache=i.cache),i.credentials&&(this.credentials=i.credentials),typeof i.timeout=="number"){if(i.timeout<1||!Number.isInteger(i.timeout))throw new _(2822,"");this.timeout=i.timeout}i.mode&&(this.mode=i.mode),i.redirect&&(this.redirect=i.redirect),i.integrity&&(this.integrity=i.integrity),i.referrer&&(this.referrer=i.referrer),i.referrerPolicy&&(this.referrerPolicy=i.referrerPolicy),this.transferCache=i.transferCache}if(this.headers??=new Ut,this.context??=new ws,!this.params)this.params=new _t,this.urlWithParams=n;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=n;else{let a=n.indexOf("?"),c=a===-1?"?":a<n.length-1?"&":"";this.urlWithParams=n+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||oh(this.body)||ih(this.body)||sh(this.body)||TE(this.body)?this.body:this.body instanceof _t?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||sh(this.body)?null:ih(this.body)?this.body.type||null:oh(this.body)?null:typeof this.body=="string"?dh:this.body instanceof _t?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?uh:null}clone(t={}){let n=t.method||this.method,r=t.url||this.url,o=t.responseType||this.responseType,i=t.keepalive??this.keepalive,s=t.priority||this.priority,a=t.cache||this.cache,c=t.mode||this.mode,l=t.redirect||this.redirect,d=t.credentials||this.credentials,u=t.referrer||this.referrer,p=t.integrity||this.integrity,f=t.referrerPolicy||this.referrerPolicy,h=t.transferCache??this.transferCache,C=t.timeout??this.timeout,y=t.body!==void 0?t.body:this.body,b=t.withCredentials??this.withCredentials,J=t.reportProgress??this.reportProgress,lt=t.headers||this.headers,ne=t.params||this.params,vr=t.context??this.context;return t.setHeaders!==void 0&&(lt=Object.keys(t.setHeaders).reduce((yr,Gt)=>yr.set(Gt,t.setHeaders[Gt]),lt)),t.setParams&&(ne=Object.keys(t.setParams).reduce((yr,Gt)=>yr.set(Gt,t.setParams[Gt]),ne)),new e(n,r,y,{params:ne,headers:lt,context:vr,reportProgress:J,responseType:o,withCredentials:b,transferCache:h,keepalive:i,cache:a,priority:s,timeout:C,mode:c,redirect:l,credentials:d,referrer:u,integrity:p,referrerPolicy:f})}},Cn=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(Cn||{}),dr=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(t,n=200,r="OK"){this.headers=t.headers||new Ut,this.status=t.status!==void 0?t.status:n,this.statusText=t.statusText||r,this.url=t.url||null,this.redirected=t.redirected,this.responseType=t.responseType,this.ok=this.status>=200&&this.status<300}},xs=class e extends dr{constructor(t={}){super(t)}type=Cn.ResponseHeader;clone(t={}){return new e({headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0})}},lo=class e extends dr{body;constructor(t={}){super(t),this.body=t.body!==void 0?t.body:null}type=Cn.Response;clone(t={}){return new e({body:t.body!==void 0?t.body:this.body,headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0,redirected:t.redirected??this.redirected,responseType:t.responseType??this.responseType})}},lr=class extends dr{name="HttpErrorResponse";message;error;ok=!1;constructor(t){super(t,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${t.url||"(unknown url)"}`:this.message=`Http failure response for ${t.url||"(unknown url)"}: ${t.status} ${t.statusText}`,this.error=t.error||null}},AE=200,NE=204;var RE=new g("");var FE=/^\)\]\}',?\n/;var ld=(()=>{class e{xhrFactory;tracingService=m(ot,{optional:!0});constructor(n){this.xhrFactory=n}maybePropagateTrace(n){return this.tracingService?.propagate?this.tracingService.propagate(n):n}handle(n){if(n.method==="JSONP")throw new _(-2800,!1);let r=this.xhrFactory;return Nn(null).pipe(Ca(()=>new F(i=>{let s=r.build();if(s.open(n.method,n.urlWithParams),n.withCredentials&&(s.withCredentials=!0),n.headers.forEach((y,b)=>s.setRequestHeader(y,b.join(","))),n.headers.has(ch)||s.setRequestHeader(ch,SE),!n.headers.has(ah)){let y=n.detectContentTypeHeader();y!==null&&s.setRequestHeader(ah,y)}if(n.timeout&&(s.timeout=n.timeout),n.responseType){let y=n.responseType.toLowerCase();s.responseType=y!=="json"?y:"text"}let a=n.serializeBody(),c=null,l=()=>{if(c!==null)return c;let y=s.statusText||"OK",b=new Ut(s.getAllResponseHeaders()),J=s.responseURL||n.url;return c=new xs({headers:b,status:s.status,statusText:y,url:J}),c},d=this.maybePropagateTrace(()=>{let{headers:y,status:b,statusText:J,url:lt}=l(),ne=null;b!==NE&&(ne=typeof s.response>"u"?s.responseText:s.response),b===0&&(b=ne?AE:0);let vr=b>=200&&b<300;if(n.responseType==="json"&&typeof ne=="string"){let yr=ne;ne=ne.replace(FE,"");try{ne=ne!==""?JSON.parse(ne):null}catch(Gt){ne=yr,vr&&(vr=!1,ne={error:Gt,text:ne})}}vr?(i.next(new lo({body:ne,headers:y,status:b,statusText:J,url:lt||void 0})),i.complete()):i.error(new lr({error:ne,headers:y,status:b,statusText:J,url:lt||void 0}))}),u=this.maybePropagateTrace(y=>{let{url:b}=l(),J=new lr({error:y,status:s.status||0,statusText:s.statusText||"Unknown Error",url:b||void 0});i.error(J)}),p=u;n.timeout&&(p=this.maybePropagateTrace(y=>{let{url:b}=l(),J=new lr({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:b||void 0});i.error(J)}));let f=!1,h=this.maybePropagateTrace(y=>{f||(i.next(l()),f=!0);let b={type:Cn.DownloadProgress,loaded:y.loaded};y.lengthComputable&&(b.total=y.total),n.responseType==="text"&&s.responseText&&(b.partialText=s.responseText),i.next(b)}),C=this.maybePropagateTrace(y=>{let b={type:Cn.UploadProgress,loaded:y.loaded};y.lengthComputable&&(b.total=y.total),i.next(b)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",p),s.addEventListener("abort",u),n.reportProgress&&(s.addEventListener("progress",h),a!==null&&s.upload&&s.upload.addEventListener("progress",C)),s.send(a),i.next({type:Cn.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",p),n.reportProgress&&(s.removeEventListener("progress",h),a!==null&&s.upload&&s.upload.removeEventListener("progress",C)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(r){return new(r||e)(M(En))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function fh(e,t){return t(e)}function OE(e,t){return(n,r)=>t.intercept(n,{handle:o=>e(o,r)})}function kE(e,t,n){return(r,o)=>Vn(n,()=>t(r,i=>e(i,o)))}var ph=new g(""),dd=new g("",{factory:()=>[]}),mh=new g(""),ud=new g("",{factory:()=>!0});function PE(){let e=null;return(t,n)=>{e===null&&(e=(m(ph,{optional:!0})??[]).reduceRight(OE,fh));let r=m(Ur);if(m(ud)){let i=r.add();return e(t,n).pipe(ei(i))}else return e(t,n)}}var fd=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:function(r){let o=null;return r?o=new(r||e):o=M(ld),o},providedIn:"root"})}return e})();var Ms=(()=>{class e{backend;injector;chain=null;pendingTasks=m(Ur);contributeToStability=m(ud);constructor(n,r){this.backend=n,this.injector=r}handle(n){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(dd),...this.injector.get(mh,[])]));this.chain=r.reduceRight((o,i)=>kE(o,i,this.injector),fh)}if(this.contributeToStability){let r=this.pendingTasks.add();return this.chain(n,o=>this.backend.handle(o)).pipe(ei(r))}else return this.chain(n,r=>this.backend.handle(r))}static \u0275fac=function(r){return new(r||e)(M(fd),M(le))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),pd=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:function(r){let o=null;return r?o=new(r||e):o=M(Ms),o},providedIn:"root"})}return e})();function cd(e,t){return{body:t,headers:e.headers,context:e.context,observe:e.observe,params:e.params,reportProgress:e.reportProgress,responseType:e.responseType,withCredentials:e.withCredentials,credentials:e.credentials,transferCache:e.transferCache,timeout:e.timeout,keepalive:e.keepalive,priority:e.priority,cache:e.cache,mode:e.mode,redirect:e.redirect,integrity:e.integrity,referrer:e.referrer,referrerPolicy:e.referrerPolicy}}var Ts=(()=>{class e{handler;constructor(n){this.handler=n}request(n,r,o={}){let i;if(n instanceof cr)i=n;else{let c;o.headers instanceof Ut?c=o.headers:c=new Ut(o.headers);let l;o.params&&(o.params instanceof _t?l=o.params:l=new _t({fromObject:o.params})),i=new cr(n,r,o.body!==void 0?o.body:null,{headers:c,context:o.context,params:l,reportProgress:o.reportProgress,responseType:o.responseType||"json",withCredentials:o.withCredentials,transferCache:o.transferCache,keepalive:o.keepalive,priority:o.priority,cache:o.cache,mode:o.mode,redirect:o.redirect,credentials:o.credentials,referrer:o.referrer,referrerPolicy:o.referrerPolicy,integrity:o.integrity,timeout:o.timeout})}let s=Nn(i).pipe(ga(c=>this.handler.handle(c)));if(n instanceof cr||o.observe==="events")return s;let a=s.pipe(ft(c=>c instanceof lo));switch(o.observe||"body"){case"body":switch(i.responseType){case"arraybuffer":return a.pipe(pe(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new _(2806,!1);return c.body}));case"blob":return a.pipe(pe(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new _(2807,!1);return c.body}));case"text":return a.pipe(pe(c=>{if(c.body!==null&&typeof c.body!="string")throw new _(2808,!1);return c.body}));default:return a.pipe(pe(c=>c.body))}case"response":return a;default:throw new _(2809,!1)}}delete(n,r={}){return this.request("DELETE",n,r)}get(n,r={}){return this.request("GET",n,r)}head(n,r={}){return this.request("HEAD",n,r)}jsonp(n,r){return this.request("JSONP",n,{params:new _t().append(r,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(n,r={}){return this.request("OPTIONS",n,r)}patch(n,r,o={}){return this.request("PATCH",n,cd(o,r))}post(n,r,o={}){return this.request("POST",n,cd(o,r))}put(n,r,o={}){return this.request("PUT",n,cd(o,r))}static \u0275fac=function(r){return new(r||e)(M(pd))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var LE=new g("",{factory:()=>!0}),VE="XSRF-TOKEN",jE=new g("",{factory:()=>VE}),BE="X-XSRF-TOKEN",HE=new g("",{factory:()=>BE}),UE=(()=>{class e{cookieName=m(jE);doc=m(Z);lastCookieString="";lastToken=null;parseCount=0;getToken(){let n=this.doc.cookie||"";return n!==this.lastCookieString&&(this.parseCount++,this.lastToken=oo(n,this.cookieName),this.lastCookieString=n),this.lastToken}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),hh=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:function(r){let o=null;return r?o=new(r||e):o=M(UE),o},providedIn:"root"})}return e})();function $E(e,t){if(!m(LE)||e.method==="GET"||e.method==="HEAD")return t(e);try{let o=m(ms).href,{origin:i}=new URL(o),{origin:s}=new URL(e.url,i);if(i!==s)return t(e)}catch{return t(e)}let n=m(hh).getToken(),r=m(HE);return n!=null&&!e.headers.has(r)&&(e=e.clone({headers:e.headers.set(r,n)})),t(e)}var md=(function(e){return e[e.Interceptors=0]="Interceptors",e[e.LegacyInterceptors=1]="LegacyInterceptors",e[e.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",e[e.NoXsrfProtection=3]="NoXsrfProtection",e[e.JsonpSupport=4]="JsonpSupport",e[e.RequestsMadeViaParent=5]="RequestsMadeViaParent",e[e.Fetch=6]="Fetch",e})(md||{});function zE(e,t){return{\u0275kind:e,\u0275providers:t}}function gh(...e){let t=[Ts,Ms,{provide:pd,useExisting:Ms},{provide:fd,useFactory:()=>m(RE,{optional:!0})??m(ld)},{provide:dd,useValue:$E,multi:!0}];for(let n of e)t.push(...n.\u0275providers);return gi(t)}var lh=new g("");function vh(){return zE(md.LegacyInterceptors,[{provide:lh,useFactory:PE},{provide:dd,useExisting:lh,multi:!0}])}var uo=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({providers:[gh(vh())]})}return e})();var Ih=(()=>{class e{_renderer;_elementRef;onChange=n=>{};onTouched=()=>{};constructor(n,r){this._renderer=n,this._elementRef=r}setProperty(n,r){this._renderer.setProperty(this._elementRef.nativeElement,n,r)}registerOnTouched(n){this.onTouched=n}registerOnChange(n){this.onChange=n}setDisabledState(n){this.setProperty("disabled",n)}static \u0275fac=function(r){return new(r||e)(j(gt),j(K))};static \u0275dir=O({type:e})}return e})(),WE=(()=>{class e extends Ih{static \u0275fac=(()=>{let n;return function(o){return(n||(n=Qi(e)))(o||e)}})();static \u0275dir=O({type:e,features:[Ie]})}return e})(),xh=new g("");var qE={provide:xh,useExisting:Mt(()=>js),multi:!0};function ZE(){let e=We()?We().getUserAgent():"";return/android (\d+)/.test(e.toLowerCase())}var YE=new g(""),js=(()=>{class e extends Ih{_compositionMode;_composing=!1;constructor(n,r,o){super(n,r),this._compositionMode=o,this._compositionMode==null&&(this._compositionMode=!ZE())}writeValue(n){let r=n??"";this.setProperty("value",r)}_handleInput(n){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(n)}_compositionStart(){this._composing=!0}_compositionEnd(n){this._composing=!1,this._compositionMode&&this.onChange(n)}static \u0275fac=function(r){return new(r||e)(j(gt),j(K),j(YE,8))};static \u0275dir=O({type:e,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(r,o){r&1&&Fe("input",function(s){return o._handleInput(s.target.value)})("blur",function(){return o.onTouched()})("compositionstart",function(){return o._compositionStart()})("compositionend",function(s){return o._compositionEnd(s.target.value)})},standalone:!1,features:[bt([qE]),Ie]})}return e})();var Mh=new g(""),Th=new g("");function Sh(e){return e!=null}function Ah(e){return rr(e)?ut(e):e}function Nh(e){let t={};return e.forEach(n=>{t=n!=null?I(I({},t),n):t}),Object.keys(t).length===0?null:t}function Rh(e,t){return t.map(n=>n(e))}function QE(e){return!e.validate}function Fh(e){return e.map(t=>QE(t)?t:n=>t.validate(n))}function KE(e){if(!e)return null;let t=e.filter(Sh);return t.length==0?null:function(n){return Nh(Rh(n,t))}}function Oh(e){return e!=null?KE(Fh(e)):null}function XE(e){if(!e)return null;let t=e.filter(Sh);return t.length==0?null:function(n){let r=Rh(n,t).map(Ah);return ma(r).pipe(pe(Nh))}}function kh(e){return e!=null?XE(Fh(e)):null}function yh(e,t){return e===null?[t]:Array.isArray(e)?[...e,t]:[e,t]}function Ph(e){return e._rawValidators}function Lh(e){return e._rawAsyncValidators}function hd(e){return e?Array.isArray(e)?e:[e]:[]}function As(e,t){return Array.isArray(e)?e.includes(t):e===t}function bh(e,t){let n=hd(t);return hd(e).forEach(o=>{As(n,o)||n.push(o)}),n}function _h(e,t){return hd(t).filter(n=>!As(e,n))}var Ns=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(t){this._rawValidators=t||[],this._composedValidatorFn=Oh(this._rawValidators)}_setAsyncValidators(t){this._rawAsyncValidators=t||[],this._composedAsyncValidatorFn=kh(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(t){this._onDestroyCallbacks.push(t)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(t=>t()),this._onDestroyCallbacks=[]}reset(t=void 0){this.control?.reset(t)}hasError(t,n){return this.control?this.control.hasError(t,n):!1}getError(t,n){return this.control?this.control.getError(t,n):null}},pr=class extends Ns{name;get formDirective(){return null}get path(){return null}},go=class extends Ns{_parent=null;name=null;valueAccessor=null},Rs=class{_cd;constructor(t){this._cd=t}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Vh=(()=>{class e extends Rs{constructor(n){super(n)}static \u0275fac=function(r){return new(r||e)(j(go,2))};static \u0275dir=O({type:e,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(r,o){r&2&&X("ng-untouched",o.isUntouched)("ng-touched",o.isTouched)("ng-pristine",o.isPristine)("ng-dirty",o.isDirty)("ng-valid",o.isValid)("ng-invalid",o.isInvalid)("ng-pending",o.isPending)},standalone:!1,features:[Ie]})}return e})(),jh=(()=>{class e extends Rs{constructor(n){super(n)}static \u0275fac=function(r){return new(r||e)(j(pr,10))};static \u0275dir=O({type:e,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(r,o){r&2&&X("ng-untouched",o.isUntouched)("ng-touched",o.isTouched)("ng-pristine",o.isPristine)("ng-dirty",o.isDirty)("ng-valid",o.isValid)("ng-invalid",o.isInvalid)("ng-pending",o.isPending)("ng-submitted",o.isSubmitted)},standalone:!1,features:[Ie]})}return e})();var fo="VALID",Ss="INVALID",ur="PENDING",po="DISABLED",$t=class{},Fs=class extends $t{value;source;constructor(t,n){super(),this.value=t,this.source=n}},mo=class extends $t{pristine;source;constructor(t,n){super(),this.pristine=t,this.source=n}},ho=class extends $t{touched;source;constructor(t,n){super(),this.touched=t,this.source=n}},fr=class extends $t{status;source;constructor(t,n){super(),this.status=t,this.source=n}},gd=class extends $t{source;constructor(t){super(),this.source=t}},Os=class extends $t{source;constructor(t){super(),this.source=t}};function Bh(e){return(Bs(e)?e.validators:e)||null}function JE(e){return Array.isArray(e)?Oh(e):e||null}function Hh(e,t){return(Bs(t)?t.asyncValidators:e)||null}function eC(e){return Array.isArray(e)?kh(e):e||null}function Bs(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}function tC(e,t,n){let r=e.controls;if(!(t?Object.keys(r):r).length)throw new _(1e3,"");if(!r[n])throw new _(1001,"")}function nC(e,t,n){e._forEachChild((r,o)=>{if(n[o]===void 0)throw new _(1002,"")})}var ks=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(t,n){this._assignValidators(t),this._assignAsyncValidators(n)}get validator(){return this._composedValidatorFn}set validator(t){this._rawValidators=this._composedValidatorFn=t}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(t){this._rawAsyncValidators=this._composedAsyncValidatorFn=t}get parent(){return this._parent}get status(){return st(this.statusReactive)}set status(t){st(()=>this.statusReactive.set(t))}_status=at(()=>this.statusReactive());statusReactive=Ne(void 0);get valid(){return this.status===fo}get invalid(){return this.status===Ss}get pending(){return this.status==ur}get disabled(){return this.status===po}get enabled(){return this.status!==po}errors;get pristine(){return st(this.pristineReactive)}set pristine(t){st(()=>this.pristineReactive.set(t))}_pristine=at(()=>this.pristineReactive());pristineReactive=Ne(!0);get dirty(){return!this.pristine}get touched(){return st(this.touchedReactive)}set touched(t){st(()=>this.touchedReactive.set(t))}_touched=at(()=>this.touchedReactive());touchedReactive=Ne(!1);get untouched(){return!this.touched}_events=new G;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(t){this._assignValidators(t)}setAsyncValidators(t){this._assignAsyncValidators(t)}addValidators(t){this.setValidators(bh(t,this._rawValidators))}addAsyncValidators(t){this.setAsyncValidators(bh(t,this._rawAsyncValidators))}removeValidators(t){this.setValidators(_h(t,this._rawValidators))}removeAsyncValidators(t){this.setAsyncValidators(_h(t,this._rawAsyncValidators))}hasValidator(t){return As(this._rawValidators,t)}hasAsyncValidator(t){return As(this._rawAsyncValidators,t)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(t={}){let n=this.touched===!1;this.touched=!0;let r=t.sourceControl??this;t.onlySelf||this._parent?.markAsTouched(P(I({},t),{sourceControl:r})),n&&t.emitEvent!==!1&&this._events.next(new ho(!0,r))}markAllAsDirty(t={}){this.markAsDirty({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(n=>n.markAllAsDirty(t))}markAllAsTouched(t={}){this.markAsTouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(n=>n.markAllAsTouched(t))}markAsUntouched(t={}){let n=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let r=t.sourceControl??this;this._forEachChild(o=>{o.markAsUntouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:r})}),t.onlySelf||this._parent?._updateTouched(t,r),n&&t.emitEvent!==!1&&this._events.next(new ho(!1,r))}markAsDirty(t={}){let n=this.pristine===!0;this.pristine=!1;let r=t.sourceControl??this;t.onlySelf||this._parent?.markAsDirty(P(I({},t),{sourceControl:r})),n&&t.emitEvent!==!1&&this._events.next(new mo(!1,r))}markAsPristine(t={}){let n=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let r=t.sourceControl??this;this._forEachChild(o=>{o.markAsPristine({onlySelf:!0,emitEvent:t.emitEvent})}),t.onlySelf||this._parent?._updatePristine(t,r),n&&t.emitEvent!==!1&&this._events.next(new mo(!0,r))}markAsPending(t={}){this.status=ur;let n=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new fr(this.status,n)),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.markAsPending(P(I({},t),{sourceControl:n}))}disable(t={}){let n=this._parentMarkedDirty(t.onlySelf);this.status=po,this.errors=null,this._forEachChild(o=>{o.disable(P(I({},t),{onlySelf:!0}))}),this._updateValue();let r=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Fs(this.value,r)),this._events.next(new fr(this.status,r)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(P(I({},t),{skipPristineCheck:n}),this),this._onDisabledChange.forEach(o=>o(!0))}enable(t={}){let n=this._parentMarkedDirty(t.onlySelf);this.status=fo,this._forEachChild(r=>{r.enable(P(I({},t),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent}),this._updateAncestors(P(I({},t),{skipPristineCheck:n}),this),this._onDisabledChange.forEach(r=>r(!1))}_updateAncestors(t,n){t.onlySelf||(this._parent?.updateValueAndValidity(t),t.skipPristineCheck||this._parent?._updatePristine({},n),this._parent?._updateTouched({},n))}setParent(t){this._parent=t}getRawValue(){return this.value}updateValueAndValidity(t={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let r=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===fo||this.status===ur)&&this._runAsyncValidator(r,t.emitEvent)}let n=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Fs(this.value,n)),this._events.next(new fr(this.status,n)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.updateValueAndValidity(P(I({},t),{sourceControl:n}))}_updateTreeValidity(t={emitEvent:!0}){this._forEachChild(n=>n._updateTreeValidity(t)),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?po:fo}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(t,n){if(this.asyncValidator){this.status=ur,this._hasOwnPendingAsyncValidator={emitEvent:n!==!1,shouldHaveEmitted:t!==!1};let r=Ah(this.asyncValidator(this));this._asyncValidationSubscription=r.subscribe(o=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(o,{emitEvent:n,shouldHaveEmitted:t})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let t=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,t}return!1}setErrors(t,n={}){this.errors=t,this._updateControlsErrors(n.emitEvent!==!1,this,n.shouldHaveEmitted)}get(t){let n=t;return n==null||(Array.isArray(n)||(n=n.split(".")),n.length===0)?null:n.reduce((r,o)=>r&&r._find(o),this)}getError(t,n){let r=n?this.get(n):this;return r?.errors?r.errors[t]:null}hasError(t,n){return!!this.getError(t,n)}get root(){let t=this;for(;t._parent;)t=t._parent;return t}_updateControlsErrors(t,n,r){this.status=this._calculateStatus(),t&&this.statusChanges.emit(this.status),(t||r)&&this._events.next(new fr(this.status,n)),this._parent&&this._parent._updateControlsErrors(t,n,r)}_initObservables(){this.valueChanges=new ce,this.statusChanges=new ce}_calculateStatus(){return this._allControlsDisabled()?po:this.errors?Ss:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ur)?ur:this._anyControlsHaveStatus(Ss)?Ss:fo}_anyControlsHaveStatus(t){return this._anyControls(n=>n.status===t)}_anyControlsDirty(){return this._anyControls(t=>t.dirty)}_anyControlsTouched(){return this._anyControls(t=>t.touched)}_updatePristine(t,n){let r=!this._anyControlsDirty(),o=this.pristine!==r;this.pristine=r,t.onlySelf||this._parent?._updatePristine(t,n),o&&this._events.next(new mo(this.pristine,n))}_updateTouched(t={},n){this.touched=this._anyControlsTouched(),this._events.next(new ho(this.touched,n)),t.onlySelf||this._parent?._updateTouched(t,n)}_onDisabledChange=[];_registerOnCollectionChange(t){this._onCollectionChange=t}_setUpdateStrategy(t){Bs(t)&&t.updateOn!=null&&(this._updateOn=t.updateOn)}_parentMarkedDirty(t){return!t&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(t){return null}_assignValidators(t){this._rawValidators=Array.isArray(t)?t.slice():t,this._composedValidatorFn=JE(this._rawValidators)}_assignAsyncValidators(t){this._rawAsyncValidators=Array.isArray(t)?t.slice():t,this._composedAsyncValidatorFn=eC(this._rawAsyncValidators)}},Ps=class extends ks{constructor(t,n,r){super(Bh(n),Hh(r,n)),this.controls=t,this._initObservables(),this._setUpdateStrategy(n),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(t,n){return this.controls[t]?this.controls[t]:(this.controls[t]=n,n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange),n)}addControl(t,n,r={}){this.registerControl(t,n),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}removeControl(t,n={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],this.updateValueAndValidity({emitEvent:n.emitEvent}),this._onCollectionChange()}setControl(t,n,r={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],n&&this.registerControl(t,n),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}contains(t){return this.controls.hasOwnProperty(t)&&this.controls[t].enabled}setValue(t,n={}){nC(this,!0,t),Object.keys(t).forEach(r=>{tC(this,!0,r),this.controls[r].setValue(t[r],{onlySelf:!0,emitEvent:n.emitEvent})}),this.updateValueAndValidity(n)}patchValue(t,n={}){t!=null&&(Object.keys(t).forEach(r=>{let o=this.controls[r];o&&o.patchValue(t[r],{onlySelf:!0,emitEvent:n.emitEvent})}),this.updateValueAndValidity(n))}reset(t={},n={}){this._forEachChild((r,o)=>{r.reset(t?t[o]:null,P(I({},n),{onlySelf:!0}))}),this._updatePristine(n,this),this._updateTouched(n,this),this.updateValueAndValidity(n),n?.emitEvent!==!1&&this._events.next(new Os(this))}getRawValue(){return this._reduceChildren({},(t,n,r)=>(t[r]=n.getRawValue(),t))}_syncPendingControls(){let t=this._reduceChildren(!1,(n,r)=>r._syncPendingControls()?!0:n);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){Object.keys(this.controls).forEach(n=>{let r=this.controls[n];r&&t(r,n)})}_setUpControls(){this._forEachChild(t=>{t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(t){for(let[n,r]of Object.entries(this.controls))if(this.contains(n)&&t(r))return!0;return!1}_reduceValue(){let t={};return this._reduceChildren(t,(n,r,o)=>((r.enabled||this.disabled)&&(n[o]=r.value),n))}_reduceChildren(t,n){let r=t;return this._forEachChild((o,i)=>{r=n(r,o,i)}),r}_allControlsDisabled(){for(let t of Object.keys(this.controls))if(this.controls[t].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(t){return this.controls.hasOwnProperty(t)?this.controls[t]:null}};var Uh=new g("",{factory:()=>vd}),vd="always";function rC(e,t){return[...t.path,e]}function Dh(e,t,n=vd){yd(e,t),t.valueAccessor.writeValue(e.value),(e.disabled||n==="always")&&t.valueAccessor.setDisabledState?.(e.disabled),iC(e,t),aC(e,t),sC(e,t),oC(e,t)}function Eh(e,t,n=!0){let r=()=>{};t?.valueAccessor?.registerOnChange(r),t?.valueAccessor?.registerOnTouched(r),Vs(e,t),e&&(t._invokeOnDestroyCallbacks(),e._registerOnCollectionChange(()=>{}))}function Ls(e,t){e.forEach(n=>{n.registerOnValidatorChange&&n.registerOnValidatorChange(t)})}function oC(e,t){if(t.valueAccessor.setDisabledState){let n=r=>{t.valueAccessor.setDisabledState(r)};e.registerOnDisabledChange(n),t._registerOnDestroy(()=>{e._unregisterOnDisabledChange(n)})}}function yd(e,t){let n=Ph(e);t.validator!==null?e.setValidators(yh(n,t.validator)):typeof n=="function"&&e.setValidators([n]);let r=Lh(e);t.asyncValidator!==null?e.setAsyncValidators(yh(r,t.asyncValidator)):typeof r=="function"&&e.setAsyncValidators([r]);let o=()=>e.updateValueAndValidity();Ls(t._rawValidators,o),Ls(t._rawAsyncValidators,o)}function Vs(e,t){let n=!1;if(e!==null){if(t.validator!==null){let o=Ph(e);if(Array.isArray(o)&&o.length>0){let i=o.filter(s=>s!==t.validator);i.length!==o.length&&(n=!0,e.setValidators(i))}}if(t.asyncValidator!==null){let o=Lh(e);if(Array.isArray(o)&&o.length>0){let i=o.filter(s=>s!==t.asyncValidator);i.length!==o.length&&(n=!0,e.setAsyncValidators(i))}}}let r=()=>{};return Ls(t._rawValidators,r),Ls(t._rawAsyncValidators,r),n}function iC(e,t){t.valueAccessor.registerOnChange(n=>{e._pendingValue=n,e._pendingChange=!0,e._pendingDirty=!0,e.updateOn==="change"&&$h(e,t)})}function sC(e,t){t.valueAccessor.registerOnTouched(()=>{e._pendingTouched=!0,e.updateOn==="blur"&&e._pendingChange&&$h(e,t),e.updateOn!=="submit"&&e.markAsTouched()})}function $h(e,t){e._pendingDirty&&e.markAsDirty(),e.setValue(e._pendingValue,{emitModelToViewChange:!1}),t.viewToModelUpdate(e._pendingValue),e._pendingChange=!1}function aC(e,t){let n=(r,o)=>{t.valueAccessor.writeValue(r),o&&t.viewToModelUpdate(r)};e.registerOnChange(n),t._registerOnDestroy(()=>{e._unregisterOnChange(n)})}function cC(e,t){e==null,yd(e,t)}function lC(e,t){return Vs(e,t)}function dC(e,t){if(!e.hasOwnProperty("model"))return!1;let n=e.model;return n.isFirstChange()?!0:!Object.is(t,n.currentValue)}function uC(e){return Object.getPrototypeOf(e.constructor)===WE}function fC(e,t){e._syncPendingControls(),t.forEach(n=>{let r=n.control;r.updateOn==="submit"&&r._pendingChange&&(n.viewToModelUpdate(r._pendingValue),r._pendingChange=!1)})}function pC(e,t){if(!t)return null;Array.isArray(t);let n,r,o;return t.forEach(i=>{i.constructor===js?n=i:uC(i)?r=i:o=i}),o||r||n||null}function mC(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function Ch(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function wh(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var vo=class extends ks{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(t=null,n,r){super(Bh(n),Hh(r,n)),this._applyFormState(t),this._setUpdateStrategy(n),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Bs(n)&&(n.nonNullable||n.initialValueIsDefault)&&(wh(t)?this.defaultValue=t.value:this.defaultValue=t)}setValue(t,n={}){this.value=this._pendingValue=t,this._onChange.length&&n.emitModelToViewChange!==!1&&this._onChange.forEach(r=>r(this.value,n.emitViewToModelChange!==!1)),this.updateValueAndValidity(n)}patchValue(t,n={}){this.setValue(t,n)}reset(t=this.defaultValue,n={}){this._applyFormState(t),this.markAsPristine(n),this.markAsUntouched(n),this.setValue(this.value,n),n.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,n?.emitEvent!==!1&&this._events.next(new Os(this))}_updateValue(){}_anyControls(t){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(t){this._onChange.push(t)}_unregisterOnChange(t){Ch(this._onChange,t)}registerOnDisabledChange(t){this._onDisabledChange.push(t)}_unregisterOnDisabledChange(t){Ch(this._onDisabledChange,t)}_forEachChild(t){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(t){wh(t)?(this.value=this._pendingValue=t.value,t.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=t}};var hC=e=>e instanceof vo;var zh=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return e})();var gC=(()=>{class e extends pr{callSetDisabledState;get submitted(){return st(this._submittedReactive)}set submitted(n){this._submittedReactive.set(n)}_submitted=at(()=>this._submittedReactive());_submittedReactive=Ne(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(n,r,o){super(),this.callSetDisabledState=o,this._setValidators(n),this._setAsyncValidators(r)}ngOnChanges(n){this.onChanges(n)}ngOnDestroy(){this.onDestroy()}onChanges(n){this._checkFormPresent(),n.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Vs(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(n){let r=this.form.get(n.path);return Dh(r,n,this.callSetDisabledState),r.updateValueAndValidity({emitEvent:!1}),this.directives.push(n),r}getControl(n){return this.form.get(n.path)}removeControl(n){Eh(n.control||null,n,!1),mC(this.directives,n)}addFormGroup(n){this._setUpFormContainer(n)}removeFormGroup(n){this._cleanUpFormContainer(n)}getFormGroup(n){return this.form.get(n.path)}getFormArray(n){return this.form.get(n.path)}addFormArray(n){this._setUpFormContainer(n)}removeFormArray(n){this._cleanUpFormContainer(n)}updateModel(n,r){this.form.get(n.path).setValue(r)}onReset(){this.resetForm()}resetForm(n=void 0,r={}){this.form.reset(n,r),this._submittedReactive.set(!1)}onSubmit(n){return this.submitted=!0,fC(this.form,this.directives),this.ngSubmit.emit(n),this.form._events.next(new gd(this.control)),n?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(n=>{let r=n.control,o=this.form.get(n.path);r!==o&&(Eh(r||null,n),hC(o)&&(Dh(o,n,this.callSetDisabledState),n.control=o))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(n){let r=this.form.get(n.path);cC(r,n),r.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(n){let r=this.form?.get(n.path);r&&lC(r,n)&&r.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){yd(this.form,this),this._oldForm&&Vs(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(r){return new(r||e)(j(Mh,10),j(Th,10),j(Uh,8))};static \u0275dir=O({type:e,features:[Ie,vn]})}return e})();var Gh=new g("");var vC={provide:go,useExisting:Mt(()=>bd)},bd=(()=>{class e extends go{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(n){}model;update=new ce;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(n,r,o,i,s){super(),this._ngModelWarningConfig=s,this._parent=n,this._setValidators(r),this._setAsyncValidators(o),this.valueAccessor=pC(this,i)}ngOnChanges(n){this._added||this._setUpControl(),dC(n,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(n){this.viewModel=n,this.update.emit(n)}get path(){return rC(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(r){return new(r||e)(j(pr,13),j(Mh,10),j(Th,10),j(xh,10),j(Gh,8))};static \u0275dir=O({type:e,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[bt([vC]),Ie,vn]})}return e})();var yC={provide:pr,useExisting:Mt(()=>_d)},_d=(()=>{class e extends gC{form=null;ngSubmit=new ce;get control(){return this.form}static \u0275fac=(()=>{let n;return function(o){return(n||(n=Qi(e)))(o||e)}})();static \u0275dir=O({type:e,selectors:[["","formGroup",""]],hostBindings:function(r,o){r&1&&Fe("submit",function(s){return o.onSubmit(s)})("reset",function(){return o.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[bt([yC]),Ie]})}return e})();var bC=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();var Hs=(()=>{class e{static withConfig(n){return{ngModule:e,providers:[{provide:Gh,useValue:n.warnOnNgModelWithFormControl??"always"},{provide:Uh,useValue:n.callSetDisabledState??vd}]}}static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[bC]})}return e})();function zt(e){return e instanceof K?e.nativeElement:e}function Wh(e){return e!=null&&`${e}`!="false"}var Dd;try{Dd=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Dd=!1}var qe=(()=>{class e{_platformId=m(yn);isBrowser=this._platformId?Ym(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Dd)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Ed;function qh(){if(Ed==null){let e=typeof document<"u"?document.head:null;Ed=!!(e&&(e.createShadowRoot||e.attachShadow))}return Ed}function Cd(e){if(qh()){let t=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&t instanceof ShadowRoot)return t}return null}function Ze(e){return e.composedPath?e.composedPath()[0]:e.target}var yo;function Zh(){if(yo==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>yo=!0}))}finally{yo=yo||!1}return yo}function mr(e){return Zh()?e:!!e.capture}var Us=new WeakMap,bo=(()=>{class e{_appRef;_injector=m(de);_environmentInjector=m(le);load(n){let r=this._appRef=this._appRef||this._injector.get(Dn),o=Us.get(r);o||(o={loaders:new Set,refs:[]},Us.set(r,o),r.onDestroy(()=>{Us.get(r)?.refs.forEach(i=>i.destroy()),Us.delete(r)})),o.loaders.has(n)||(o.loaders.add(n),o.refs.push(Gm(n,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Yh=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();function _o(e){return e.buttons===0||e.detail===0}function Do(e){let t=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!t&&t.identifier===-1&&(t.radiusX==null||t.radiusX===1)&&(t.radiusY==null||t.radiusY===1)}var Qh=new g("cdk-input-modality-detector-options"),Kh={ignoreKeys:[18,17,224,91,16]},Xh=650,wd={passive:!0,capture:!0},Jh=(()=>{class e{_platform=m(qe);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Qt(null);_options;_lastTouchMs=0;_onKeydown=n=>{this._options?.ignoreKeys?.some(r=>r===n.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Ze(n))};_onMousedown=n=>{Date.now()-this._lastTouchMs<Xh||(this._modality.next(_o(n)?"keyboard":"mouse"),this._mostRecentTarget=Ze(n))};_onTouchstart=n=>{if(Do(n)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Ze(n)};constructor(){let n=m(N),r=m(Z),o=m(Qh,{optional:!0});if(this._options=I(I({},Kh),o),this.modalityDetected=this._modality.pipe(Da(1)),this.modalityChanged=this.modalityDetected.pipe(va()),this._platform.isBrowser){let i=m(Ue).createRenderer(null,null);this._listenerCleanups=n.runOutsideAngular(()=>[i.listen(r,"keydown",this._onKeydown,wd),i.listen(r,"mousedown",this._onMousedown,wd),i.listen(r,"touchstart",this._onTouchstart,wd)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(n=>n())}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Eo=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(Eo||{}),eg=new g("cdk-focus-monitor-default-options"),$s=mr({passive:!0,capture:!0}),Id=(()=>{class e{_ngZone=m(N);_platform=m(qe);_inputModalityDetector=m(Jh);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=m(Z);_stopInputModalityDetector=new G;constructor(){let n=m(eg,{optional:!0});this._detectionMode=n?.detectionMode||Eo.IMMEDIATE}_rootNodeFocusAndBlurListener=n=>{let r=Ze(n);for(let o=r;o;o=o.parentElement)n.type==="focus"?this._onFocus(n,o):this._onBlur(n,o)};monitor(n,r=!1){let o=zt(n);if(!this._platform.isBrowser||o.nodeType!==1)return Nn();let i=Cd(o)||this._document,s=this._elementInfo.get(o);if(s)return r&&(s.checkChildren=!0),s.subject;let a={checkChildren:r,subject:new G,rootNode:i};return this._elementInfo.set(o,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(n){let r=zt(n),o=this._elementInfo.get(r);o&&(o.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(o))}focusVia(n,r,o){let i=zt(n),s=this._document.activeElement;i===s?this._getClosestElementsInfo(i).forEach(([a,c])=>this._originChanged(a,r,c)):(this._setOrigin(r),typeof i.focus=="function"&&i.focus(o))}ngOnDestroy(){this._elementInfo.forEach((n,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(n){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(n)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:n&&this._isLastInteractionFromInputLabel(n)?"mouse":"program"}_shouldBeAttributedToTouch(n){return this._detectionMode===Eo.EVENTUAL||!!n?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(n,r){n.classList.toggle("cdk-focused",!!r),n.classList.toggle("cdk-touch-focused",r==="touch"),n.classList.toggle("cdk-keyboard-focused",r==="keyboard"),n.classList.toggle("cdk-mouse-focused",r==="mouse"),n.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(n,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=n,this._originFromTouchInteraction=n==="touch"&&r,this._detectionMode===Eo.IMMEDIATE){clearTimeout(this._originTimeoutId);let o=this._originFromTouchInteraction?Xh:1;this._originTimeoutId=setTimeout(()=>this._origin=null,o)}})}_onFocus(n,r){let o=this._elementInfo.get(r),i=Ze(n);!o||!o.checkChildren&&r!==i||this._originChanged(r,this._getFocusOrigin(i),o)}_onBlur(n,r){let o=this._elementInfo.get(r);!o||o.checkChildren&&n.relatedTarget instanceof Node&&r.contains(n.relatedTarget)||(this._setClasses(r),this._emitOrigin(o,null))}_emitOrigin(n,r){n.subject.observers.length&&this._ngZone.run(()=>n.subject.next(r))}_registerGlobalListeners(n){if(!this._platform.isBrowser)return;let r=n.rootNode,o=this._rootNodeFocusListenerCount.get(r)||0;o||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,$s),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,$s)}),this._rootNodeFocusListenerCount.set(r,o+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Xt(this._stopInputModalityDetector)).subscribe(i=>{this._setOrigin(i,!0)}))}_removeGlobalListeners(n){let r=n.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let o=this._rootNodeFocusListenerCount.get(r);o>1?this._rootNodeFocusListenerCount.set(r,o-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,$s),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,$s),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(n,r,o){this._setClasses(n,r),this._emitOrigin(o,r),this._lastFocusOrigin=r}_getClosestElementsInfo(n){let r=[];return this._elementInfo.forEach((o,i)=>{(i===n||o.checkChildren&&i.contains(n))&&r.push([i,o])}),r}_isLastInteractionFromInputLabel(n){let{_mostRecentTarget:r,mostRecentModality:o}=this._inputModalityDetector;if(o!=="mouse"||!r||r===n||n.nodeName!=="INPUT"&&n.nodeName!=="TEXTAREA"||n.disabled)return!1;let i=n.labels;if(i){for(let s=0;s<i.length;s++)if(i[s].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var tg=new Set,wn,xd=(()=>{class e{_platform=m(qe);_nonce=m(tr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):EC}matchMedia(n){return(this._platform.WEBKIT||this._platform.BLINK)&&DC(n,this._nonce),this._matchMedia(n)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function DC(e,t){if(!tg.has(e))try{wn||(wn=document.createElement("style"),t&&wn.setAttribute("nonce",t),wn.setAttribute("type","text/css"),document.head.appendChild(wn)),wn.sheet&&(wn.sheet.insertRule(`@media ${e} {body{ }}`,0),tg.add(e))}catch(n){console.error(n)}}function EC(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var CC=(()=>{class e{create(n){return typeof MutationObserver>"u"?null:new MutationObserver(n)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ng=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({providers:[CC]})}return e})();var Md={},hr=class e{_appId=m(er);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(t,n=!1){return this._appId!=="ng"&&(t+=this._appId),Md.hasOwnProperty(t)||(Md[t]=0),`${t}${n?e._infix+"-":""}${Md[t]++}`}static \u0275fac=function(n){return new(n||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})};var wC=new g("cdk-dir-doc",{providedIn:"root",factory:()=>m(Z)}),IC=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function rg(e){let t=e?.toLowerCase()||"";return t==="auto"&&typeof navigator<"u"&&navigator?.language?IC.test(navigator.language)?"rtl":"ltr":t==="rtl"?"rtl":"ltr"}var Td=(()=>{class e{get value(){return this.valueSignal()}valueSignal=Ne("ltr");change=new ce;constructor(){let n=m(wC,{optional:!0});if(n){let r=n.body?n.body.dir:null,o=n.documentElement?n.documentElement.dir:null;this.valueSignal.set(rg(r||o||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ct=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();var Sd=class{_box;_destroyed=new G;_resizeSubject=new G;_resizeObserver;_elementObservables=new Map;constructor(t){this._box=t,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(n=>this._resizeSubject.next(n)))}observe(t){return this._elementObservables.has(t)||this._elementObservables.set(t,new F(n=>{let r=this._resizeSubject.subscribe(n);return this._resizeObserver?.observe(t,{box:this._box}),()=>{this._resizeObserver?.unobserve(t),r.unsubscribe(),this._elementObservables.delete(t)}}).pipe(ft(n=>n.some(r=>r.target===t)),_a({bufferSize:1,refCount:!0}),Xt(this._destroyed))),this._elementObservables.get(t)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},og=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=m(N);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,n]of this._observers)n.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(n,r){let o=r?.box||"content-box";return this._observers.has(o)||this._observers.set(o,new Sd(o)),this._observers.get(o).observe(n)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var xC=new g("MATERIAL_ANIMATIONS"),ig=null;function MC(){return m(xC,{optional:!0})?.animationsDisabled||m(ul,{optional:!0})==="NoopAnimations"?"di-disabled":(ig??=m(xd).matchMedia("(prefers-reduced-motion)").matches,ig?"reduced-motion":"enabled")}function gr(){return MC()!=="enabled"}var TC=["notch"],SC=["matFormFieldNotchedOutline",""],AC=["*"],sg=["iconPrefixContainer"],ag=["textPrefixContainer"],cg=["iconSuffixContainer"],lg=["textSuffixContainer"],NC=["textField"],RC=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],FC=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function OC(e,t){e&1&&Re(0,"span",21)}function kC(e,t){if(e&1&&(B(0,"label",20),te(1,1),_e(2,OC,1,0,"span",21),H()),e&2){let n=Ge(2);ze("floating",n._shouldLabelFloat())("monitorResize",n._hasOutline())("id",n._labelId),it("for",n._control.disableAutomaticLabeling?null:n._control.id),Y(2),De(!n.hideRequiredMarker&&n._control.required?2:-1)}}function PC(e,t){if(e&1&&_e(0,kC,3,5,"label",20),e&2){let n=Ge();De(n._hasFloatingLabel()?0:-1)}}function LC(e,t){e&1&&Re(0,"div",7)}function VC(e,t){}function jC(e,t){if(e&1&&jt(0,VC,0,0,"ng-template",13),e&2){Ge(2);let n=ds(1);ze("ngTemplateOutlet",n)}}function BC(e,t){if(e&1&&(B(0,"div",9),_e(1,jC,1,1,null,13),H()),e&2){let n=Ge();ze("matFormFieldNotchedOutlineOpen",n._shouldLabelFloat()),Y(),De(n._forceDisplayInfixLabel()?-1:1)}}function HC(e,t){e&1&&(B(0,"div",10,2),te(2,2),H())}function UC(e,t){e&1&&(B(0,"div",11,3),te(2,3),H())}function $C(e,t){}function zC(e,t){if(e&1&&jt(0,$C,0,0,"ng-template",13),e&2){Ge();let n=ds(1);ze("ngTemplateOutlet",n)}}function GC(e,t){e&1&&(B(0,"div",14,4),te(2,4),H())}function WC(e,t){e&1&&(B(0,"div",15,5),te(2,5),H())}function qC(e,t){e&1&&Re(0,"div",16)}function ZC(e,t){e&1&&(B(0,"div",18),te(1,6),H())}function YC(e,t){if(e&1&&(B(0,"mat-hint",22),xe(1),H()),e&2){let n=Ge(2);ze("id",n._hintLabelId),Y(),or(n.hintLabel)}}function QC(e,t){if(e&1&&(B(0,"div",19),_e(1,YC,2,2,"mat-hint",22),te(2,7),Re(3,"div",23),te(4,8),H()),e&2){let n=Ge();Y(),De(n.hintLabel?1:-1)}}var Ad=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e,selectors:[["mat-label"]]})}return e})(),KC=new g("MatError");var Nd=(()=>{class e{align="start";id=m(hr).getId("mat-mdc-hint-");static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(r,o){r&2&&(Xr("id",o.id),it("align",null),X("mat-mdc-form-field-hint-end",o.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),XC=new g("MatPrefix");var JC=new g("MatSuffix");var gg=new g("FloatingLabelParent"),dg=(()=>{class e{_elementRef=m(K);get floating(){return this._floating}set floating(n){this._floating=n,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(n){this._monitorResize=n,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=m(og);_ngZone=m(N);_parent=m(gg);_resizeSubscription=new se;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return ew(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(r,o){r&2&&X("mdc-floating-label--float-above",o.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function ew(e){let t=e;if(t.offsetParent!==null)return t.scrollWidth;let n=t.cloneNode(!0);n.style.setProperty("position","absolute"),n.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(n);let r=n.scrollWidth;return n.remove(),r}var ug="mdc-line-ripple--active",zs="mdc-line-ripple--deactivating",fg=(()=>{class e{_elementRef=m(K);_cleanupTransitionEnd;constructor(){let n=m(N),r=m(gt);n.runOutsideAngular(()=>{this._cleanupTransitionEnd=r.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let n=this._elementRef.nativeElement.classList;n.remove(zs),n.add(ug)}deactivate(){this._elementRef.nativeElement.classList.add(zs)}_handleTransitionEnd=n=>{let r=this._elementRef.nativeElement.classList,o=r.contains(zs);n.propertyName==="opacity"&&o&&r.remove(ug,zs)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),pg=(()=>{class e{_elementRef=m(K);_ngZone=m(N);open=!1;_notch;ngAfterViewInit(){let n=this._elementRef.nativeElement,r=n.querySelector(".mdc-floating-label");r?(n.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(r.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>r.style.transitionDuration="")}))):n.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(n){let r=this._notch.nativeElement;!this.open||!n?r.style.width="":r.style.width=`calc(${n}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(n){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${n}px)`)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=oe({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(r,o){if(r&1&&eo(TC,5),r&2){let i;ge(i=ve())&&(o._notch=i.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(r,o){r&2&&X("mdc-notched-outline--notched",o.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:SC,ngContentSelectors:AC,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(r,o){r&1&&(yt(),vt(0,"div",1),Bt(1,"div",2,0),te(3),Ht(),vt(4,"div",3))},encapsulation:2,changeDetection:0})}return e})(),tw=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e})}return e})();var nw=new g("MatFormField"),rw=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),mg="fill",ow="auto",hg="fixed",iw="translateY(-50%)",vg=(()=>{class e{_elementRef=m(K);_changeDetectorRef=m(sr);_platform=m(qe);_idGenerator=m(hr);_ngZone=m(N);_defaults=m(rw,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=no("iconPrefixContainer");_textPrefixContainerSignal=no("textPrefixContainer");_iconSuffixContainerSignal=no("iconSuffixContainer");_textSuffixContainerSignal=no("textSuffixContainer");_prefixSuffixContainers=at(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(n=>n?.nativeElement).filter(n=>n!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Bm(Ad);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(n){this._hideRequiredMarker=Wh(n)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||ow}set floatLabel(n){n!==this._floatLabel&&(this._floatLabel=n,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(n){let r=n||this._defaults?.appearance||mg;this._appearanceSignal.set(r)}_appearanceSignal=Ne(mg);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||hg}set subscriptSizing(n){this._subscriptSizing=n||this._defaults?.subscriptSizing||hg}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(n){this._hintLabel=n,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(n){this._explicitFormFieldControl=n}_destroyed=new G;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=gr();constructor(){let n=this._defaults,r=m(Td);n&&(n.appearance&&(this.appearance=n.appearance),this._hideRequiredMarker=!!n?.hideRequiredMarker,n.color&&(this.color=n.color)),Ic(()=>this._currentDirection=r.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=at(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(n){let r=this._control,o="mat-mdc-form-field-type-";n&&this._elementRef.nativeElement.classList.remove(o+n.controlType),r.controlType&&this._elementRef.nativeElement.classList.add(o+r.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=r.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=r.stateChanges.pipe(Ea([void 0,void 0]),pe(()=>[r.errorState,r.userAriaDescribedBy]),ya(),ft(([[i,s],[a,c]])=>i!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),r.ngControl&&r.ngControl.valueChanges&&(this._valueChanges=r.ngControl.valueChanges.pipe(Xt(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(n=>!n._isText),this._hasTextPrefix=!!this._prefixChildren.find(n=>n._isText),this._hasIconSuffix=!!this._suffixChildren.find(n=>!n._isText),this._hasTextSuffix=!!this._suffixChildren.find(n=>n._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),ha(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let n=this._control.focused;n&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!n&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",n),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",n)}_syncOutlineLabelOffset(){zm({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let n of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(n,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:n=>this._writeOutlinedLabelStyles(n())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=at(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(n){let r=this._control?this._control.ngControl:null;return r&&r[n]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let n=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&n.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let i=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;i?n.push(i.id):this._hintLabel&&n.push(this._hintLabelId),s&&n.push(s.id)}else this._errorChildren&&n.push(...this._errorChildren.map(i=>i.id));let r=this._control.describedByIds,o;if(r){let i=this._describedByIds||n;o=n.concat(r.filter(s=>s&&!i.includes(s)))}else o=n;this._control.setDescribedByIds(o),this._describedByIds=n}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let n=this._iconPrefixContainer?.nativeElement,r=this._textPrefixContainer?.nativeElement,o=this._iconSuffixContainer?.nativeElement,i=this._textSuffixContainer?.nativeElement,s=n?.getBoundingClientRect().width??0,a=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,l=i?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,h=`var(--mat-mdc-form-field-label-transform, ${iw} translateX(${f}))`,C=s+a+c+l;return[h,C]}_writeOutlinedLabelStyles(n){if(n!==null){let[r,o]=n;this._floatingLabel&&(this._floatingLabel.element.style.transform=r),o!==null&&this._notchedOutline?._setMaxWidth(o)}}_isAttachedToDom(){let n=this._elementRef.nativeElement;if(n.getRootNode){let r=n.getRootNode();return r&&r!==n}return document.documentElement.contains(n)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=oe({type:e,selectors:[["mat-form-field"]],contentQueries:function(r,o,i){if(r&1&&(as(i,o._labelChild,Ad,5),ss(i,tw,5)(i,XC,5)(i,JC,5)(i,KC,5)(i,Nd,5)),r&2){ls();let s;ge(s=ve())&&(o._formFieldControl=s.first),ge(s=ve())&&(o._prefixChildren=s),ge(s=ve())&&(o._suffixChildren=s),ge(s=ve())&&(o._errorChildren=s),ge(s=ve())&&(o._hintChildren=s)}},viewQuery:function(r,o){if(r&1&&(cs(o._iconPrefixContainerSignal,sg,5)(o._textPrefixContainerSignal,ag,5)(o._iconSuffixContainerSignal,cg,5)(o._textSuffixContainerSignal,lg,5),eo(NC,5)(sg,5)(ag,5)(cg,5)(lg,5)(dg,5)(pg,5)(fg,5)),r&2){ls(4);let i;ge(i=ve())&&(o._textField=i.first),ge(i=ve())&&(o._iconPrefixContainer=i.first),ge(i=ve())&&(o._textPrefixContainer=i.first),ge(i=ve())&&(o._iconSuffixContainer=i.first),ge(i=ve())&&(o._textSuffixContainer=i.first),ge(i=ve())&&(o._floatingLabel=i.first),ge(i=ve())&&(o._notchedOutline=i.first),ge(i=ve())&&(o._lineRipple=i.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(r,o){r&2&&X("mat-mdc-form-field-label-always-float",o._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",o._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",o._hasIconSuffix)("mat-form-field-invalid",o._control.errorState)("mat-form-field-disabled",o._control.disabled)("mat-form-field-autofilled",o._control.autofilled)("mat-form-field-appearance-fill",o.appearance=="fill")("mat-form-field-appearance-outline",o.appearance=="outline")("mat-form-field-hide-placeholder",o._hasFloatingLabel()&&!o._shouldLabelFloat())("mat-primary",o.color!=="accent"&&o.color!=="warn")("mat-accent",o.color==="accent")("mat-warn",o.color==="warn")("ng-untouched",o._shouldForward("untouched"))("ng-touched",o._shouldForward("touched"))("ng-pristine",o._shouldForward("pristine"))("ng-dirty",o._shouldForward("dirty"))("ng-valid",o._shouldForward("valid"))("ng-invalid",o._shouldForward("invalid"))("ng-pending",o._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[bt([{provide:nw,useExisting:e},{provide:gg,useExisting:e}])],ngContentSelectors:FC,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(r,o){if(r&1&&(yt(RC),jt(0,PC,1,1,"ng-template",null,0,Ul),B(2,"div",6,1),Fe("click",function(s){return o._control.onContainerClick(s)}),_e(4,LC,1,0,"div",7),B(5,"div",8),_e(6,BC,2,2,"div",9),_e(7,HC,3,0,"div",10),_e(8,UC,3,0,"div",11),B(9,"div",12),_e(10,zC,1,1,null,13),te(11),H(),_e(12,GC,3,0,"div",14),_e(13,WC,3,0,"div",15),H(),_e(14,qC,1,0,"div",16),H(),B(15,"div",17),_e(16,ZC,2,0,"div",18)(17,QC,5,1,"div",19),H()),r&2){let i;Y(2),X("mdc-text-field--filled",!o._hasOutline())("mdc-text-field--outlined",o._hasOutline())("mdc-text-field--no-label",!o._hasFloatingLabel())("mdc-text-field--disabled",o._control.disabled)("mdc-text-field--invalid",o._control.errorState),Y(2),De(!o._hasOutline()&&!o._control.disabled?4:-1),Y(2),De(o._hasOutline()?6:-1),Y(),De(o._hasIconPrefix?7:-1),Y(),De(o._hasTextPrefix?8:-1),Y(2),De(!o._hasOutline()||o._forceDisplayInfixLabel()?10:-1),Y(2),De(o._hasTextSuffix?12:-1),Y(),De(o._hasIconSuffix?13:-1),Y(),De(o._hasOutline()?-1:14),Y(),X("mat-mdc-form-field-subscript-dynamic-size",o.subscriptSizing==="dynamic");let s=o._getSubscriptMessageType();Y(),De((i=s)==="error"?16:i==="hint"?17:-1)}},dependencies:[dg,pg,Xl,fg,Nd],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return e})();var Co=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[ng,vg,ct]})}return e})();var yg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[Co,Co,Yh,ct]})}return e})();var Oe=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(Oe||{}),Rd=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Oe.HIDDEN;constructor(t,n,r,o=!1){this._renderer=t,this.element=n,this.config=r,this._animationForciblyDisabledThroughCss=o}fadeOut(){this._renderer.fadeOutRipple(this)}},bg=mr({passive:!0,capture:!0}),Fd=class{_events=new Map;addHandler(t,n,r,o){let i=this._events.get(n);if(i){let s=i.get(r);s?s.add(o):i.set(r,new Set([o]))}else this._events.set(n,new Map([[r,new Set([o])]])),t.runOutsideAngular(()=>{document.addEventListener(n,this._delegateEventHandler,bg)})}removeHandler(t,n,r){let o=this._events.get(t);if(!o)return;let i=o.get(n);i&&(i.delete(r),i.size===0&&o.delete(n),o.size===0&&(this._events.delete(t),document.removeEventListener(t,this._delegateEventHandler,bg)))}_delegateEventHandler=t=>{let n=Ze(t);n&&this._events.get(t.type)?.forEach((r,o)=>{(o===n||o.contains(n))&&r.forEach(i=>i.handleEvent(t))})}},wo={enterDuration:225,exitDuration:150},sw=800,_g=mr({passive:!0,capture:!0}),Dg=["mousedown","touchstart"],Eg=["mouseup","mouseleave","touchend","touchcancel"],aw=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=oe({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,o){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return e})(),Gs=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Fd;constructor(t,n,r,o,i){this._target=t,this._ngZone=n,this._platform=o,o.isBrowser&&(this._containerElement=zt(r)),i&&i.get(bo).load(aw)}fadeInRipple(t,n,r={}){let o=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),i=I(I({},wo),r.animation);r.centered&&(t=o.left+o.width/2,n=o.top+o.height/2);let s=r.radius||cw(t,n,o),a=t-o.left,c=n-o.top,l=i.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),p=u.transitionProperty,f=u.transitionDuration,h=p==="none"||f==="0s"||f==="0s, 0s"||o.width===0&&o.height===0,C=new Rd(this,d,r,h);d.style.transform="scale3d(1, 1, 1)",C.state=Oe.FADING_IN,r.persistent||(this._mostRecentTransientRipple=C);let y=null;return!h&&(l||i.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let b=()=>{y&&(y.fallbackTimer=null),clearTimeout(lt),this._finishRippleTransition(C)},J=()=>this._destroyRipple(C),lt=setTimeout(J,l+100);d.addEventListener("transitionend",b),d.addEventListener("transitioncancel",J),y={onTransitionEnd:b,onTransitionCancel:J,fallbackTimer:lt}}),this._activeRipples.set(C,y),(h||!l)&&this._finishRippleTransition(C),C}fadeOutRipple(t){if(t.state===Oe.FADING_OUT||t.state===Oe.HIDDEN)return;let n=t.element,r=I(I({},wo),t.config.animation);n.style.transitionDuration=`${r.exitDuration}ms`,n.style.opacity="0",t.state=Oe.FADING_OUT,(t._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(t)}fadeOutAll(){this._getActiveRipples().forEach(t=>t.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(t=>{t.config.persistent||t.fadeOut()})}setupTriggerEvents(t){let n=zt(t);!this._platform.isBrowser||!n||n===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=n,Dg.forEach(r=>{e._eventManager.addHandler(this._ngZone,r,n,this)}))}handleEvent(t){t.type==="mousedown"?this._onMousedown(t):t.type==="touchstart"?this._onTouchStart(t):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Eg.forEach(n=>{this._triggerElement.addEventListener(n,this,_g)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(t){t.state===Oe.FADING_IN?this._startFadeOutTransition(t):t.state===Oe.FADING_OUT&&this._destroyRipple(t)}_startFadeOutTransition(t){let n=t===this._mostRecentTransientRipple,{persistent:r}=t.config;t.state=Oe.VISIBLE,!r&&(!n||!this._isPointerDown)&&t.fadeOut()}_destroyRipple(t){let n=this._activeRipples.get(t)??null;this._activeRipples.delete(t),this._activeRipples.size||(this._containerRect=null),t===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),t.state=Oe.HIDDEN,n!==null&&(t.element.removeEventListener("transitionend",n.onTransitionEnd),t.element.removeEventListener("transitioncancel",n.onTransitionCancel),n.fallbackTimer!==null&&clearTimeout(n.fallbackTimer)),t.element.remove()}_onMousedown(t){let n=_o(t),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+sw;!this._target.rippleDisabled&&!n&&!r&&(this._isPointerDown=!0,this.fadeInRipple(t.clientX,t.clientY,this._target.rippleConfig))}_onTouchStart(t){if(!this._target.rippleDisabled&&!Do(t)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let n=t.changedTouches;if(n)for(let r=0;r<n.length;r++)this.fadeInRipple(n[r].clientX,n[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(t=>{let n=t.state===Oe.VISIBLE||t.config.terminateOnPointerUp&&t.state===Oe.FADING_IN;!t.config.persistent&&n&&t.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let t=this._triggerElement;t&&(Dg.forEach(n=>e._eventManager.removeHandler(n,t,this)),this._pointerUpEventsRegistered&&(Eg.forEach(n=>t.removeEventListener(n,this,_g)),this._pointerUpEventsRegistered=!1))}};function cw(e,t,n){let r=Math.max(Math.abs(e-n.left),Math.abs(e-n.right)),o=Math.max(Math.abs(t-n.top),Math.abs(t-n.bottom));return Math.sqrt(r*r+o*o)}var Cg=new g("mat-ripple-global-options");var lw={capture:!0},dw=["focus","mousedown","mouseenter","touchstart"],Od="mat-ripple-loader-uninitialized",kd="mat-ripple-loader-class-name",wg="mat-ripple-loader-centered",Ws="mat-ripple-loader-disabled",Ig=(()=>{class e{_document=m(Z);_animationsDisabled=gr();_globalRippleOptions=m(Cg,{optional:!0});_platform=m(qe);_ngZone=m(N);_injector=m(de);_eventCleanups;_hosts=new Map;constructor(){let n=m(Ue).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>dw.map(r=>n.listen(this._document,r,this._onInteraction,lw)))}ngOnDestroy(){let n=this._hosts.keys();for(let r of n)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(n,r){n.setAttribute(Od,this._globalRippleOptions?.namespace??""),(r.className||!n.hasAttribute(kd))&&n.setAttribute(kd,r.className||""),r.centered&&n.setAttribute(wg,""),r.disabled&&n.setAttribute(Ws,"")}setDisabled(n,r){let o=this._hosts.get(n);o?(o.target.rippleDisabled=r,!r&&!o.hasSetUpEvents&&(o.hasSetUpEvents=!0,o.renderer.setupTriggerEvents(n))):r?n.setAttribute(Ws,""):n.removeAttribute(Ws)}_onInteraction=n=>{let r=Ze(n);if(r instanceof HTMLElement){let o=r.closest(`[${Od}="${this._globalRippleOptions?.namespace??""}"]`);o&&this._createRipple(o)}};_createRipple(n){if(!this._document||this._hosts.has(n))return;n.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",n.getAttribute(kd)),n.append(r);let o=this._globalRippleOptions,i=this._animationsDisabled?0:o?.animation?.enterDuration??wo.enterDuration,s=this._animationsDisabled?0:o?.animation?.exitDuration??wo.exitDuration,a={rippleDisabled:this._animationsDisabled||o?.disabled||n.hasAttribute(Ws),rippleConfig:{centered:n.hasAttribute(wg),terminateOnPointerUp:o?.terminateOnPointerUp,animation:{enterDuration:i,exitDuration:s}}},c=new Gs(a,this._ngZone,r,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(n),this._hosts.set(n,{target:a,renderer:c,hasSetUpEvents:l}),n.removeAttribute(Od)}destroyRipple(n){let r=this._hosts.get(n);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(n))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var xg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=oe({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,o){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var uw=new g("MAT_BUTTON_CONFIG");function Mg(e){return e==null?void 0:Um(e)}var Tg=(()=>{class e{_elementRef=m(K);_ngZone=m(N);_animationsDisabled=gr();_config=m(uw,{optional:!0});_focusMonitor=m(Id);_cleanupClick;_renderer=m(gt);_rippleLoader=m(Ig);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(n){this._disableRipple=n,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(n){this._disabled=n,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(n){this.tabIndex=n}constructor(){m(bo).load(xg);let n=this._elementRef.nativeElement;this._isAnchor=n.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(n,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(n="program",r){n?this._focusMonitor.focusVia(this._elementRef.nativeElement,n,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",n=>{this.disabled&&(n.preventDefault(),n.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=O({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(r,o){r&2&&(it("disabled",o._getDisabledAttribute())("aria-disabled",o._getAriaDisabled())("tabindex",o._getTabIndex()),Hl(o.color?"mat-"+o.color:""),X("mat-mdc-button-disabled",o.disabled)("mat-mdc-button-disabled-interactive",o.disabledInteractive)("mat-unthemed",!o.color)("_mat-animation-noopable",o._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",ar],disabled:[2,"disabled","disabled",ar],ariaDisabled:[2,"aria-disabled","ariaDisabled",ar],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ar],tabIndex:[2,"tabIndex","tabIndex",Mg],_tabindex:[2,"tabindex","_tabindex",Mg]}})}return e})();var Sg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[ct]})}return e})();var fw=["matButton",""],pw=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],mw=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var Ag=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Ng=(()=>{class e extends Tg{get appearance(){return this._appearance}set appearance(n){this.setAppearance(n||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let n=hw(this._elementRef.nativeElement);n&&this.setAppearance(n)}setAppearance(n){if(n===this._appearance)return;let r=this._elementRef.nativeElement.classList,o=this._appearance?Ag.get(this._appearance):null,i=Ag.get(n);o&&r.remove(...o),r.add(...i),this._appearance=n}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=oe({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Ie],attrs:fw,ngContentSelectors:mw,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(r,o){r&1&&(yt(pw),vt(0,"span",0),te(1),Bt(2,"span",1),te(3,1),Ht(),te(4,2),vt(5,"span",2)(6,"span",3)),r&2&&X("mdc-button__ripple",!o._isFab)("mdc-fab__ripple",o._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();function hw(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var Rg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[Sg,ct]})}return e})();var vw=["*"];var yw=new g("MAT_CARD_CONFIG"),Fg=(()=>{class e{appearance;constructor(){let n=m(yw,{optional:!0});this.appearance=n?.appearance||"raised"}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=oe({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(r,o){r&2&&X("mat-mdc-card-outlined",o.appearance==="outlined")("mdc-card--outlined",o.appearance==="outlined")("mat-mdc-card-filled",o.appearance==="filled")("mdc-card--filled",o.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:vw,decls:1,vars:0,template:function(r,o){r&1&&(yt(),te(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return e})();var Og=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[ct]})}return e})();var kg={production:!0,apiUrl:"https://zooweb20260403171703.azurewebsites.net/api"};var qs=class e{constructor(t){this.http=t}apiUrl=`${kg.apiUrl}/animals`;getAnimals(){return this.http.get(this.apiUrl).pipe(Ir(t=>console.log("Fetched animals:",t)),Fn(t=>(console.error("Error fetching animals",t),wr(()=>t))))}addAnimal(t){return this.http.post(this.apiUrl,t).pipe(Ir(n=>console.log("Added animal:",n)),Fn(n=>(console.error("Error adding animal",n),wr(()=>n))))}deleteAnimal(t){return this.http.delete(`${this.apiUrl}/${t}`).pipe(Ir(()=>console.log("Deleted animal:",t)),Fn(n=>(console.error("Error deleting animal",n),wr(()=>n))))}static \u0275fac=function(n){return new(n||e)(M(Ts))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})};function Dw(e,t){if(e&1){let n=Bl();B(0,"mat-card",6)(1,"h3"),xe(2),H(),B(3,"p")(4,"strong"),xe(5,"Species:"),H(),xe(6),H(),B(7,"p")(8,"strong"),xe(9,"Health Status:"),H(),xe(10),H(),B(11,"button",7),Fe("click",function(){let o=Ei(n).$implicit,i=Ge();return Ci(i.removeAnimal(o.id))}),xe(12," Remove "),H()()}if(e&2){let n=t.$implicit;Y(2),or(n.name),Y(4),ir(" ",n.species),Y(4),ir(" ",n.healthStatus)}}var Zs=class e{constructor(t,n){this.animalService=t;this.cdr=n}animalForm=new Ps({name:new vo(""),species:new vo(""),healthStatus:new vo("")});animals=[];ngOnInit(){this.loadAnimals()}loadAnimals(){this.animalService.getAnimals().subscribe({next:t=>{this.animals=t,this.cdr.detectChanges()},error:t=>console.error(t)})}onSubmit(){if(this.animalForm.valid){let t=this.animalForm.value;this.animalService.addAnimal(t).subscribe({next:n=>{this.animals.push(n),this.animalForm.reset(),this.cdr.detectChanges()}})}}removeAnimal(t){this.animalService.deleteAnimal(t).subscribe({next:()=>{this.animals=this.animals.filter(n=>n.id!==t),this.cdr.detectChanges()}})}static \u0275fac=function(n){return new(n||e)(j(qs),j(sr))};static \u0275cmp=oe({type:e,selectors:[["app-add-animal"]],decls:8,vars:2,consts:[[3,"ngSubmit","formGroup"],["type","text","formControlName","name","placeholder","Name"],["type","text","formControlName","species","placeholder","Species"],["type","text","formControlName","healthStatus","placeholder","Health Status"],["mat-raised-button","","color","primary","type","submit"],["style","margin-top: 10px;",4,"ngFor","ngForOf"],[2,"margin-top","10px"],["mat-raised-button","","color","warn",3,"click"]],template:function(n,r){n&1&&(B(0,"form",0),Fe("ngSubmit",function(){return r.onSubmit()}),Re(1,"input",1)(2,"input",2)(3,"input",3),B(4,"button",4),xe(5," Add Animal "),H()(),Re(6,"hr"),jt(7,Dw,13,3,"mat-card",5)),n&2&&(ze("formGroup",r.animalForm),Y(7),ze("ngForOf",r.animals))},dependencies:[vs,gs,Hs,zh,js,Vh,jh,_d,bd,uo,yg,Rg,Ng,Og,Fg,Co],encapsulation:2})};var Ys=class e{static \u0275fac=function(n){return new(n||e)};static \u0275cmp=oe({type:e,selectors:[["app-root"]],decls:3,vars:0,template:function(n,r){n&1&&(B(0,"h1"),xe(1,"Zoo Animal Management"),H(),Re(2,"app-add-animal"))},dependencies:[Zs],encapsulation:2})};ad(Ys,{providers:[vi(uo,Hs)]}).catch(e=>console.error(e));
