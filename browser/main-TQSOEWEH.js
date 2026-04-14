var zg=Object.defineProperty,Gg=Object.defineProperties;var Wg=Object.getOwnPropertyDescriptors;var $d=Object.getOwnPropertySymbols;var qg=Object.prototype.hasOwnProperty,Zg=Object.prototype.propertyIsEnumerable;var zd=(e,t,n)=>t in e?zg(e,t,{enumerable:!0,configurable:!0,writable:!0,value:n}):e[t]=n,I=(e,t)=>{for(var n in t||={})qg.call(t,n)&&zd(e,n,t[n]);if($d)for(var n of $d(t))Zg.call(t,n)&&zd(e,n,t[n]);return e},N=(e,t)=>Gg(e,Wg(t));var me=null,Io=!1,ta=1,Yg=null,ae=Symbol("SIGNAL");function w(e){let t=me;return me=e,t}function So(){return me}var Wt={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Mn(e){if(Io)throw new Error("");if(me===null)return;me.consumerOnSignalRead(e);let t=me.producersTail;if(t!==void 0&&t.producer===e)return;let n,r=me.recomputing;if(r&&(n=t!==void 0?t.nextProducer:me.producers,n!==void 0&&n.producer===e)){me.producersTail=n,n.lastReadVersion=e.version;return}let o=e.consumersTail;if(o!==void 0&&o.consumer===me&&(!r||Kg(o,me)))return;let i=Sn(me),s={producer:e,consumer:me,nextProducer:n,prevConsumer:o,lastReadVersion:e.version,nextConsumer:void 0};me.producersTail=s,t!==void 0?t.nextProducer=s:me.producers=s,i&&Zd(e,s)}function Gd(){ta++}function Ao(e){if(!(Sn(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===ta)){if(!e.producerMustRecompute(e)&&!Tn(e)){To(e);return}e.producerRecomputeValue(e),To(e)}}function na(e){if(e.consumers===void 0)return;let t=Io;Io=!0;try{for(let n=e.consumers;n!==void 0;n=n.nextConsumer){let r=n.consumer;r.dirty||Qg(r)}}finally{Io=t}}function ra(){return me?.consumerAllowSignalWrites!==!1}function Qg(e){e.dirty=!0,na(e),e.consumerMarkedDirty?.(e)}function To(e){e.dirty=!1,e.lastCleanEpoch=ta}function Dt(e){return e&&Wd(e),w(e)}function Wd(e){e.producersTail=void 0,e.recomputing=!0}function qt(e,t){w(t),e&&qd(e)}function qd(e){e.recomputing=!1;let t=e.producersTail,n=t!==void 0?t.nextProducer:e.producers;if(n!==void 0){if(Sn(e))do n=oa(n);while(n!==void 0);t!==void 0?t.nextProducer=void 0:e.producers=void 0}}function Tn(e){for(let t=e.producers;t!==void 0;t=t.nextProducer){let n=t.producer,r=t.lastReadVersion;if(r!==n.version||(Ao(n),r!==n.version))return!0}return!1}function Et(e){if(Sn(e)){let t=e.producers;for(;t!==void 0;)t=oa(t)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function Zd(e,t){let n=e.consumersTail,r=Sn(e);if(n!==void 0?(t.nextConsumer=n.nextConsumer,n.nextConsumer=t):(t.nextConsumer=void 0,e.consumers=t),t.prevConsumer=n,e.consumersTail=t,!r)for(let o=e.producers;o!==void 0;o=o.nextProducer)Zd(o.producer,o)}function oa(e){let t=e.producer,n=e.nextProducer,r=e.nextConsumer,o=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,r!==void 0?r.prevConsumer=o:t.consumersTail=o,o!==void 0)o.nextConsumer=r;else if(t.consumers=r,!Sn(t)){let i=t.producers;for(;i!==void 0;)i=oa(i)}return n}function Sn(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function No(e){Yg?.(e)}function Kg(e,t){let n=t.producersTail;if(n!==void 0){let r=t.producers;do{if(r===e)return!0;if(r===n)break;r=r.nextProducer}while(r!==void 0)}return!1}function Ro(e,t){return Object.is(e,t)}function _r(e,t){let n=Object.create(Xg);n.computation=e,t!==void 0&&(n.equal=t);let r=()=>{if(Ao(n),Mn(n),n.value===br)throw n.error;return n.value};return r[ae]=n,No(n),r}var xo=Symbol("UNSET"),Mo=Symbol("COMPUTING"),br=Symbol("ERRORED"),Xg=N(I({},Wt),{value:xo,dirty:!0,error:null,equal:Ro,kind:"computed",producerMustRecompute(e){return e.value===xo||e.value===Mo},producerRecomputeValue(e){if(e.value===Mo)throw new Error("");let t=e.value;e.value=Mo;let n=Dt(e),r,o=!1;try{r=e.computation(),w(null),o=t!==xo&&t!==br&&r!==br&&e.equal(t,r)}catch(i){r=br,e.error=i}finally{qt(e,n)}if(o){e.value=t;return}e.value=r,e.version++}});function Jg(){throw new Error}var Yd=Jg;function Qd(e){Yd(e)}function ia(e){Yd=e}var ev=null;function sa(e,t){let n=Object.create(Fo);n.value=e,t!==void 0&&(n.equal=t);let r=()=>Kd(n);return r[ae]=n,No(n),[r,s=>Dr(n,s),s=>aa(n,s)]}function Kd(e){return Mn(e),e.value}function Dr(e,t){ra()||Qd(e),e.equal(e.value,t)||(e.value=t,tv(e))}function aa(e,t){ra()||Qd(e),Dr(e,t(e.value))}var Fo=N(I({},Wt),{equal:Ro,value:void 0,kind:"signal"});function tv(e){e.version++,Gd(),na(e),ev?.(e)}var ca=N(I({},Wt),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function la(e){if(e.dirty=!1,e.version>0&&!Tn(e))return;e.version++;let t=Dt(e);try{e.cleanup(),e.fn()}finally{qt(e,t)}}function S(e){return typeof e=="function"}function Oo(e){let n=e(r=>{Error.call(r),r.stack=new Error().stack});return n.prototype=Object.create(Error.prototype),n.prototype.constructor=n,n}var ko=Oo(e=>function(n){e(this),this.message=n?`${n.length} errors occurred during unsubscription:
${n.map((r,o)=>`${o+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=n});function Zt(e,t){if(e){let n=e.indexOf(t);0<=n&&e.splice(n,1)}}var te=class e{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:n}=this;if(n)if(this._parentage=null,Array.isArray(n))for(let i of n)i.remove(this);else n.remove(this);let{initialTeardown:r}=this;if(S(r))try{r()}catch(i){t=i instanceof ko?i.errors:[i]}let{_finalizers:o}=this;if(o){this._finalizers=null;for(let i of o)try{Xd(i)}catch(s){t=t??[],s instanceof ko?t=[...t,...s.errors]:t.push(s)}}if(t)throw new ko(t)}}add(t){var n;if(t&&t!==this)if(this.closed)Xd(t);else{if(t instanceof e){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(n=this._finalizers)!==null&&n!==void 0?n:[]).push(t)}}_hasParent(t){let{_parentage:n}=this;return n===t||Array.isArray(n)&&n.includes(t)}_addParent(t){let{_parentage:n}=this;this._parentage=Array.isArray(n)?(n.push(t),n):n?[n,t]:t}_removeParent(t){let{_parentage:n}=this;n===t?this._parentage=null:Array.isArray(n)&&Zt(n,t)}remove(t){let{_finalizers:n}=this;n&&Zt(n,t),t instanceof e&&t._removeParent(this)}};te.EMPTY=(()=>{let e=new te;return e.closed=!0,e})();var da=te.EMPTY;function Po(e){return e instanceof te||e&&"closed"in e&&S(e.remove)&&S(e.add)&&S(e.unsubscribe)}function Xd(e){S(e)?e():e.unsubscribe()}var ke={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var An={setTimeout(e,t,...n){let{delegate:r}=An;return r?.setTimeout?r.setTimeout(e,t,...n):setTimeout(e,t,...n)},clearTimeout(e){let{delegate:t}=An;return(t?.clearTimeout||clearTimeout)(e)},delegate:void 0};function Lo(e){An.setTimeout(()=>{let{onUnhandledError:t}=ke;if(t)t(e);else throw e})}function Yt(){}var Jd=ua("C",void 0,void 0);function eu(e){return ua("E",void 0,e)}function tu(e){return ua("N",e,void 0)}function ua(e,t,n){return{kind:e,value:t,error:n}}var Qt=null;function Nn(e){if(ke.useDeprecatedSynchronousErrorHandling){let t=!Qt;if(t&&(Qt={errorThrown:!1,error:null}),e(),t){let{errorThrown:n,error:r}=Qt;if(Qt=null,n)throw r}}else e()}function nu(e){ke.useDeprecatedSynchronousErrorHandling&&Qt&&(Qt.errorThrown=!0,Qt.error=e)}var Kt=class extends te{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,Po(t)&&t.add(this)):this.destination=ov}static create(t,n,r){return new ut(t,n,r)}next(t){this.isStopped?ma(tu(t),this):this._next(t)}error(t){this.isStopped?ma(eu(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?ma(Jd,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},nv=Function.prototype.bind;function fa(e,t){return nv.call(e,t)}var pa=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:n}=this;if(n.next)try{n.next(t)}catch(r){Vo(r)}}error(t){let{partialObserver:n}=this;if(n.error)try{n.error(t)}catch(r){Vo(r)}else Vo(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(n){Vo(n)}}},ut=class extends Kt{constructor(t,n,r){super();let o;if(S(t)||!t)o={next:t??void 0,error:n??void 0,complete:r??void 0};else{let i;this&&ke.useDeprecatedNextContext?(i=Object.create(t),i.unsubscribe=()=>this.unsubscribe(),o={next:t.next&&fa(t.next,i),error:t.error&&fa(t.error,i),complete:t.complete&&fa(t.complete,i)}):o=t}this.destination=new pa(o)}};function Vo(e){ke.useDeprecatedSynchronousErrorHandling?nu(e):Lo(e)}function rv(e){throw e}function ma(e,t){let{onStoppedNotification:n}=ke;n&&An.setTimeout(()=>n(e,t))}var ov={closed:!0,next:Yt,error:rv,complete:Yt};var Rn=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Fn(e){return e}function ru(e){return e.length===0?Fn:e.length===1?e[0]:function(n){return e.reduce((r,o)=>o(r),n)}}var O=(()=>{class e{constructor(n){n&&(this._subscribe=n)}lift(n){let r=new e;return r.source=this,r.operator=n,r}subscribe(n,r,o){let i=sv(n)?n:new ut(n,r,o);return Nn(()=>{let{operator:s,source:a}=this;i.add(s?s.call(i,a):a?this._subscribe(i):this._trySubscribe(i))}),i}_trySubscribe(n){try{return this._subscribe(n)}catch(r){n.error(r)}}forEach(n,r){return r=ou(r),new r((o,i)=>{let s=new ut({next:a=>{try{n(a)}catch(c){i(c),s.unsubscribe()}},error:i,complete:o});this.subscribe(s)})}_subscribe(n){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(n)}[Rn](){return this}pipe(...n){return ru(n)(this)}toPromise(n){return n=ou(n),new n((r,o)=>{let i;this.subscribe(s=>i=s,s=>o(s),()=>r(i))})}}return e.create=t=>new e(t),e})();function ou(e){var t;return(t=e??ke.Promise)!==null&&t!==void 0?t:Promise}function iv(e){return e&&S(e.next)&&S(e.error)&&S(e.complete)}function sv(e){return e&&e instanceof Kt||iv(e)&&Po(e)}function av(e){return S(e?.lift)}function L(e){return t=>{if(av(t))return t.lift(function(n){try{return e(n,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function z(e,t,n,r,o){return new ha(e,t,n,r,o)}var ha=class extends Kt{constructor(t,n,r,o,i,s){super(t),this.onFinalize=i,this.shouldUnsubscribe=s,this._next=n?function(a){try{n(a)}catch(c){t.error(c)}}:super._next,this._error=o?function(a){try{o(a)}catch(c){t.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:n}=this;super.unsubscribe(),!n&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};var iu=Oo(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var G=(()=>{class e extends O{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let r=new jo(this,this);return r.operator=n,r}_throwIfClosed(){if(this.closed)throw new iu}next(n){Nn(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(n)}})}error(n){Nn(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:r}=this;for(;r.length;)r.shift().error(n)}})}complete(){Nn(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:r,isStopped:o,observers:i}=this;return r||o?da:(this.currentObservers=null,i.push(n),new te(()=>{this.currentObservers=null,Zt(i,n)}))}_checkFinalizedStatuses(n){let{hasError:r,thrownError:o,isStopped:i}=this;r?n.error(o):i&&n.complete()}asObservable(){let n=new O;return n.source=this,n}}return e.create=(t,n)=>new jo(t,n),e})(),jo=class extends G{constructor(t,n){super(),this.destination=t,this.source=n}next(t){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.next)===null||r===void 0||r.call(n,t)}error(t){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.error)===null||r===void 0||r.call(n,t)}complete(){var t,n;(n=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||n===void 0||n.call(t)}_subscribe(t){var n,r;return(r=(n=this.source)===null||n===void 0?void 0:n.subscribe(t))!==null&&r!==void 0?r:da}};var Xt=class extends G{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let n=super._subscribe(t);return!n.closed&&t.next(this._value),n}getValue(){let{hasError:t,thrownError:n,_value:r}=this;if(t)throw n;return this._throwIfClosed(),r}next(t){super.next(this._value=t)}};var Er={now(){return(Er.delegate||Date).now()},delegate:void 0};var Bo=class extends G{constructor(t=1/0,n=1/0,r=Er){super(),this._bufferSize=t,this._windowTime=n,this._timestampProvider=r,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=n===1/0,this._bufferSize=Math.max(1,t),this._windowTime=Math.max(1,n)}next(t){let{isStopped:n,_buffer:r,_infiniteTimeWindow:o,_timestampProvider:i,_windowTime:s}=this;n||(r.push(t),!o&&r.push(i.now()+s)),this._trimBuffer(),super.next(t)}_subscribe(t){this._throwIfClosed(),this._trimBuffer();let n=this._innerSubscribe(t),{_infiniteTimeWindow:r,_buffer:o}=this,i=o.slice();for(let s=0;s<i.length&&!t.closed;s+=r?1:2)t.next(i[s]);return this._checkFinalizedStatuses(t),n}_trimBuffer(){let{_bufferSize:t,_timestampProvider:n,_buffer:r,_infiniteTimeWindow:o}=this,i=(o?1:2)*t;if(t<1/0&&i<r.length&&r.splice(0,r.length-i),!o){let s=n.now(),a=0;for(let c=1;c<r.length&&r[c]<=s;c+=2)a=c;a&&r.splice(0,a+1)}}};var Ho=class extends te{constructor(t,n){super()}schedule(t,n=0){return this}};var Cr={setInterval(e,t,...n){let{delegate:r}=Cr;return r?.setInterval?r.setInterval(e,t,...n):setInterval(e,t,...n)},clearInterval(e){let{delegate:t}=Cr;return(t?.clearInterval||clearInterval)(e)},delegate:void 0};var Uo=class extends Ho{constructor(t,n){super(t,n),this.scheduler=t,this.work=n,this.pending=!1}schedule(t,n=0){var r;if(this.closed)return this;this.state=t;let o=this.id,i=this.scheduler;return o!=null&&(this.id=this.recycleAsyncId(i,o,n)),this.pending=!0,this.delay=n,this.id=(r=this.id)!==null&&r!==void 0?r:this.requestAsyncId(i,this.id,n),this}requestAsyncId(t,n,r=0){return Cr.setInterval(t.flush.bind(t,this),r)}recycleAsyncId(t,n,r=0){if(r!=null&&this.delay===r&&this.pending===!1)return n;n!=null&&Cr.clearInterval(n)}execute(t,n){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let r=this._execute(t,n);if(r)return r;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(t,n){let r=!1,o;try{this.work(t)}catch(i){r=!0,o=i||new Error("Scheduled action threw falsy error")}if(r)return this.unsubscribe(),o}unsubscribe(){if(!this.closed){let{id:t,scheduler:n}=this,{actions:r}=n;this.work=this.state=this.scheduler=null,this.pending=!1,Zt(r,this),t!=null&&(this.id=this.recycleAsyncId(n,t,null)),this.delay=null,super.unsubscribe()}}};var On=class e{constructor(t,n=e.now){this.schedulerActionCtor=t,this.now=n}schedule(t,n=0,r){return new this.schedulerActionCtor(this,t).schedule(r,n)}};On.now=Er.now;var $o=class extends On{constructor(t,n=On.now){super(t,n),this.actions=[],this._active=!1}flush(t){let{actions:n}=this;if(this._active){n.push(t);return}let r;this._active=!0;do if(r=t.execute(t.state,t.delay))break;while(t=n.shift());if(this._active=!1,r){for(;t=n.shift();)t.unsubscribe();throw r}}};var ga=new $o(Uo),su=ga;var zo=new O(e=>e.complete());function Go(e){return e&&S(e.schedule)}function va(e){return e[e.length-1]}function au(e){return S(va(e))?e.pop():void 0}function Ct(e){return Go(va(e))?e.pop():void 0}function cu(e,t){return typeof va(e)=="number"?e.pop():t}function du(e,t,n,r){function o(i){return i instanceof n?i:new n(function(s){s(i)})}return new(n||(n=Promise))(function(i,s){function a(d){try{l(r.next(d))}catch(u){s(u)}}function c(d){try{l(r.throw(d))}catch(u){s(u)}}function l(d){d.done?i(d.value):o(d.value).then(a,c)}l((r=r.apply(e,t||[])).next())})}function lu(e){var t=typeof Symbol=="function"&&Symbol.iterator,n=t&&e[t],r=0;if(n)return n.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&r>=e.length&&(e=void 0),{value:e&&e[r++],done:!e}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function Jt(e){return this instanceof Jt?(this.v=e,this):new Jt(e)}function uu(e,t,n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=n.apply(e,t||[]),o,i=[];return o=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),o[Symbol.asyncIterator]=function(){return this},o;function s(f){return function(h){return Promise.resolve(h).then(f,u)}}function a(f,h){r[f]&&(o[f]=function(C){return new Promise(function(y,b){i.push([f,C,y,b])>1||c(f,C)})},h&&(o[f]=h(o[f])))}function c(f,h){try{l(r[f](h))}catch(C){m(i[0][3],C)}}function l(f){f.value instanceof Jt?Promise.resolve(f.value.v).then(d,u):m(i[0][2],f)}function d(f){c("next",f)}function u(f){c("throw",f)}function m(f,h){f(h),i.shift(),i.length&&c(i[0][0],i[0][1])}}function fu(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=e[Symbol.asyncIterator],n;return t?t.call(e):(e=typeof lu=="function"?lu(e):e[Symbol.iterator](),n={},r("next"),r("throw"),r("return"),n[Symbol.asyncIterator]=function(){return this},n);function r(i){n[i]=e[i]&&function(s){return new Promise(function(a,c){s=e[i](s),o(a,c,s.done,s.value)})}}function o(i,s,a,c){Promise.resolve(c).then(function(l){i({value:l,done:a})},s)}}var Wo=e=>e&&typeof e.length=="number"&&typeof e!="function";function qo(e){return S(e?.then)}function Zo(e){return S(e[Rn])}function Yo(e){return Symbol.asyncIterator&&S(e?.[Symbol.asyncIterator])}function Qo(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function cv(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ko=cv();function Xo(e){return S(e?.[Ko])}function Jo(e){return uu(this,arguments,function*(){let n=e.getReader();try{for(;;){let{value:r,done:o}=yield Jt(n.read());if(o)return yield Jt(void 0);yield yield Jt(r)}}finally{n.releaseLock()}})}function ei(e){return S(e?.getReader)}function W(e){if(e instanceof O)return e;if(e!=null){if(Zo(e))return lv(e);if(Wo(e))return dv(e);if(qo(e))return uv(e);if(Yo(e))return mu(e);if(Xo(e))return fv(e);if(ei(e))return mv(e)}throw Qo(e)}function lv(e){return new O(t=>{let n=e[Rn]();if(S(n.subscribe))return n.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function dv(e){return new O(t=>{for(let n=0;n<e.length&&!t.closed;n++)t.next(e[n]);t.complete()})}function uv(e){return new O(t=>{e.then(n=>{t.closed||(t.next(n),t.complete())},n=>t.error(n)).then(null,Lo)})}function fv(e){return new O(t=>{for(let n of e)if(t.next(n),t.closed)return;t.complete()})}function mu(e){return new O(t=>{pv(e,t).catch(n=>t.error(n))})}function mv(e){return mu(Jo(e))}function pv(e,t){var n,r,o,i;return du(this,void 0,void 0,function*(){try{for(n=fu(e);r=yield n.next(),!r.done;){let s=r.value;if(t.next(s),t.closed)return}}catch(s){o={error:s}}finally{try{r&&!r.done&&(i=n.return)&&(yield i.call(n))}finally{if(o)throw o.error}}t.complete()})}function Me(e,t,n,r=0,o=!1){let i=t.schedule(function(){n(),o?e.add(this.schedule(null,r)):this.unsubscribe()},r);if(e.add(i),!o)return i}function ti(e,t=0){return L((n,r)=>{n.subscribe(z(r,o=>Me(r,e,()=>r.next(o),t),()=>Me(r,e,()=>r.complete(),t),o=>Me(r,e,()=>r.error(o),t)))})}function ni(e,t=0){return L((n,r)=>{r.add(e.schedule(()=>n.subscribe(r),t))})}function pu(e,t){return W(e).pipe(ni(t),ti(t))}function hu(e,t){return W(e).pipe(ni(t),ti(t))}function gu(e,t){return new O(n=>{let r=0;return t.schedule(function(){r===e.length?n.complete():(n.next(e[r++]),n.closed||this.schedule())})})}function vu(e,t){return new O(n=>{let r;return Me(n,t,()=>{r=e[Ko](),Me(n,t,()=>{let o,i;try{({value:o,done:i}=r.next())}catch(s){n.error(s);return}i?n.complete():n.next(o)},0,!0)}),()=>S(r?.return)&&r.return()})}function ri(e,t){if(!e)throw new Error("Iterable cannot be null");return new O(n=>{Me(n,t,()=>{let r=e[Symbol.asyncIterator]();Me(n,t,()=>{r.next().then(o=>{o.done?n.complete():n.next(o.value)})},0,!0)})})}function yu(e,t){return ri(Jo(e),t)}function bu(e,t){if(e!=null){if(Zo(e))return pu(e,t);if(Wo(e))return gu(e,t);if(qo(e))return hu(e,t);if(Yo(e))return ri(e,t);if(Xo(e))return vu(e,t);if(ei(e))return yu(e,t)}throw Qo(e)}function ft(e,t){return t?bu(e,t):W(e)}function Ye(...e){let t=Ct(e);return ft(e,t)}function _u(e){return e instanceof Date&&!isNaN(e)}function Q(e,t){return L((n,r)=>{let o=0;n.subscribe(z(r,i=>{r.next(e.call(t,i,o++))}))})}var{isArray:hv}=Array;function gv(e,t){return hv(t)?e(...t):e(t)}function Du(e){return Q(t=>gv(e,t))}var{isArray:vv}=Array,{getPrototypeOf:yv,prototype:bv,keys:_v}=Object;function Eu(e){if(e.length===1){let t=e[0];if(vv(t))return{args:t,keys:null};if(Dv(t)){let n=_v(t);return{args:n.map(r=>t[r]),keys:n}}}return{args:e,keys:null}}function Dv(e){return e&&typeof e=="object"&&yv(e)===bv}function Cu(e,t){return e.reduce((n,r,o)=>(n[r]=t[o],n),{})}function wu(e,t,n,r,o,i,s,a){let c=[],l=0,d=0,u=!1,m=()=>{u&&!c.length&&!l&&t.complete()},f=C=>l<r?h(C):c.push(C),h=C=>{i&&t.next(C),l++;let y=!1;W(n(C,d++)).subscribe(z(t,b=>{o?.(b),i?f(b):t.next(b)},()=>{y=!0},void 0,()=>{if(y)try{for(l--;c.length&&l<r;){let b=c.shift();s?Me(t,s,()=>h(b)):h(b)}m()}catch(b){t.error(b)}}))};return e.subscribe(z(t,f,()=>{u=!0,m()})),()=>{a?.()}}function wt(e,t,n=1/0){return S(t)?wt((r,o)=>Q((i,s)=>t(r,i,o,s))(W(e(r,o))),n):(typeof t=="number"&&(n=t),L((r,o)=>wu(r,o,e,n)))}function oi(e=1/0){return wt(Fn,e)}function Iu(){return oi(1)}function wr(...e){return Iu()(ft(e,Ct(e)))}function ya(...e){let t=au(e),{args:n,keys:r}=Eu(e),o=new O(i=>{let{length:s}=n;if(!s){i.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;W(n[d]).subscribe(z(i,m=>{u||(u=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!u)&&(l||i.next(r?Cu(r,a):a),i.complete())}))}});return t?o.pipe(Du(t)):o}function xu(e=0,t,n=su){let r=-1;return t!=null&&(Go(t)?n=t:r=t),new O(o=>{let i=_u(e)?+e-n.now():e;i<0&&(i=0);let s=0;return n.schedule(function(){o.closed||(o.next(s++),0<=r?this.schedule(void 0,r):o.complete())},i)})}function ba(...e){let t=Ct(e),n=cu(e,1/0),r=e;return r.length?r.length===1?W(r[0]):oi(n)(ft(r,t)):zo}function mt(e,t){return L((n,r)=>{let o=0;n.subscribe(z(r,i=>e.call(t,i,o++)&&r.next(i)))})}function _a(e,t){return S(t)?wt(e,t,1):wt(e,1)}function Da(e){return e<=0?()=>zo:L((t,n)=>{let r=0;t.subscribe(z(n,o=>{++r<=e&&(n.next(o),e<=r&&n.complete())}))})}function Mu(){return L((e,t)=>{e.subscribe(z(t,Yt))})}function Tu(e){return Q(()=>e)}function Ea(e,t){return t?n=>wr(t.pipe(Da(1),Mu()),n.pipe(Ea(e))):wt((n,r)=>W(e(n,r)).pipe(Da(1),Tu(n)))}function Ir(e,t=ga){let n=xu(e,t);return Ea(()=>n)}function Ca(e,t=Fn){return e=e??Ev,L((n,r)=>{let o,i=!0;n.subscribe(z(r,s=>{let a=t(s);(i||!e(o,a))&&(i=!1,o=a,r.next(s))}))})}function Ev(e,t){return e===t}function ii(e){return L((t,n)=>{try{t.subscribe(n)}finally{n.add(e)}})}function wa(){return L((e,t)=>{let n,r=!1;e.subscribe(z(t,o=>{let i=n;n=o,r&&t.next([i,o]),r=!0}))})}function Su(e={}){let{connector:t=()=>new G,resetOnError:n=!0,resetOnComplete:r=!0,resetOnRefCountZero:o=!0}=e;return i=>{let s,a,c,l=0,d=!1,u=!1,m=()=>{a?.unsubscribe(),a=void 0},f=()=>{m(),s=c=void 0,d=u=!1},h=()=>{let C=s;f(),C?.unsubscribe()};return L((C,y)=>{l++,!u&&!d&&m();let b=c=c??t();y.add(()=>{l--,l===0&&!u&&!d&&(a=Ia(h,o))}),b.subscribe(y),!s&&l>0&&(s=new ut({next:ee=>b.next(ee),error:ee=>{u=!0,m(),a=Ia(f,n,ee),b.error(ee)},complete:()=>{d=!0,m(),a=Ia(f,r),b.complete()}}),W(C).subscribe(s))})(i)}}function Ia(e,t,...n){if(t===!0){e();return}if(t===!1)return;let r=new ut({next:()=>{r.unsubscribe(),e()}});return W(t(...n)).subscribe(r)}function xa(e,t,n){let r,o=!1;return e&&typeof e=="object"?{bufferSize:r=1/0,windowTime:t=1/0,refCount:o=!1,scheduler:n}=e:r=e??1/0,Su({connector:()=>new Bo(r,t,n),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:o})}function Ma(e){return mt((t,n)=>e<=n)}function Ta(...e){let t=Ct(e);return L((n,r)=>{(t?wr(e,n,t):wr(e,n)).subscribe(r)})}function Sa(e,t){return L((n,r)=>{let o=null,i=0,s=!1,a=()=>s&&!o&&r.complete();n.subscribe(z(r,c=>{o?.unsubscribe();let l=0,d=i++;W(e(c,d)).subscribe(o=z(r,u=>r.next(t?t(c,u,d,l++):u),()=>{o=null,a()}))},()=>{s=!0,a()}))})}function en(e){return L((t,n)=>{W(e).subscribe(z(n,()=>n.complete(),Yt)),!n.closed&&t.subscribe(n)})}var Aa;function si(){return Aa}function Qe(e){let t=Aa;return Aa=e,t}var Au=Symbol("NotFound");function kn(e){return e===Au||e?.name==="\u0275NotFound"}function Nu(e){let t=w(null);try{return e()}finally{w(t)}}var $a="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",_=class extends Error{code;constructor(t,n){super(Rr(t,n)),this.code=t}};function Cv(e){return`NG0${Math.abs(e)}`}function Rr(e,t){return`${Cv(e)}${t?": "+t:""}`}function V(e){for(let t in e)if(e[t]===V)return t;throw Error("")}function Pu(e,t){for(let n in t)t.hasOwnProperty(n)&&!e.hasOwnProperty(n)&&(e[n]=t[n])}function mi(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(mi).join(", ")}]`;if(e==null)return""+e;let t=e.overriddenName||e.name;if(t)return`${t}`;let n=e.toString();if(n==null)return""+n;let r=n.indexOf(`
`);return r>=0?n.slice(0,r):n}function pi(e,t){return e?t?`${e} ${t}`:e:t||""}var wv=V({__forward_ref__:V});function Mt(e){return e.__forward_ref__=Mt,e}function ce(e){return za(e)?e():e}function za(e){return typeof e=="function"&&e.hasOwnProperty(wv)&&e.__forward_ref__===Mt}function v(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function U(e){return{providers:e.providers||[],imports:e.imports||[]}}function hi(e){return Iv(e,gi)}function Iv(e,t){return e.hasOwnProperty(t)&&e[t]||null}function xv(e){let t=e?.[gi]??null;return t||null}function Ra(e){return e&&e.hasOwnProperty(ci)?e[ci]:null}var gi=V({\u0275prov:V}),ci=V({\u0275inj:V}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(t,n){this._desc=t,this.\u0275prov=void 0,typeof n=="number"?this.__NG_ELEMENT_ID__=n:n!==void 0&&(this.\u0275prov=v({token:this,providedIn:n.providedIn||"root",factory:n.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Ga(e){return e&&!!e.\u0275providers}var Wa=V({\u0275cmp:V}),qa=V({\u0275dir:V}),Za=V({\u0275pipe:V});var Mr=V({\u0275fac:V}),an=V({__NG_ELEMENT_ID__:V}),Ru=V({__NG_ENV_ID__:V});function Tt(e){return Qa(e,"@Component"),e[Wa]||null}function Ya(e){return Qa(e,"@Directive"),e[qa]||null}function Lu(e){return Qa(e,"@Pipe"),e[Za]||null}function Qa(e,t){if(e==null)throw new _(-919,!1)}function Ka(e){return typeof e=="string"?e:e==null?"":String(e)}var Vu=V({ngErrorCode:V}),Mv=V({ngErrorMessage:V}),Tv=V({ngTokenPath:V});function Xa(e,t){return ju("",-200,t)}function vi(e,t){throw new _(-201,!1)}function ju(e,t,n){let r=new _(t,e);return r[Vu]=t,r[Mv]=e,n&&(r[Tv]=n),r}function Sv(e){return e[Vu]}var Fa;function Bu(){return Fa}function Ce(e){let t=Fa;return Fa=e,t}function Ja(e,t,n){let r=hi(e);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(n&8)return null;if(t!==void 0)return t;vi(e,"")}var Av={},tn=Av,Nv="__NG_DI_FLAG__",Oa=class{injector;constructor(t){this.injector=t}retrieve(t,n){let r=nn(n)||0;try{return this.injector.get(t,r&8?null:tn,r)}catch(o){if(kn(o))return o;throw o}}};function Rv(e,t=0){let n=si();if(n===void 0)throw new _(-203,!1);if(n===null)return Ja(e,void 0,t);{let r=Fv(t),o=n.retrieve(e,r);if(kn(o)){if(r.optional)return null;throw o}return o}}function M(e,t=0){return(Bu()||Rv)(ce(e),t)}function p(e,t){return M(e,nn(t))}function nn(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function Fv(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function ka(e){let t=[];for(let n=0;n<e.length;n++){let r=ce(e[n]);if(Array.isArray(r)){if(r.length===0)throw new _(900,!1);let o,i=0;for(let s=0;s<r.length;s++){let a=r[s],c=Ov(a);typeof c=="number"?c===-1?o=a.token:i|=c:o=a}t.push(M(o,i))}else t.push(M(r))}return t}function Ov(e){return e[Nv]}function rn(e,t){let n=e.hasOwnProperty(Mr);return n?e[Mr]:null}function Hu(e,t,n){if(e.length!==t.length)return!1;for(let r=0;r<e.length;r++){let o=e[r],i=t[r];if(n&&(o=n(o),i=n(i)),i!==o)return!1}return!0}function Uu(e){return e.flat(Number.POSITIVE_INFINITY)}function yi(e,t){e.forEach(n=>Array.isArray(n)?yi(n,t):t(n))}function ec(e,t,n){t>=e.length?e.push(n):e.splice(t,0,n)}function Fr(e,t){return t>=e.length-1?e.pop():e.splice(t,1)[0]}function $u(e,t){let n=[];for(let r=0;r<e;r++)n.push(t);return n}function zu(e,t,n,r){let o=e.length;if(o==t)e.push(n,r);else if(o===1)e.push(r,e[0]),e[0]=n;else{for(o--,e.push(e[o-1],e[o]);o>t;){let i=o-2;e[o]=e[i],o--}e[t]=n,e[t+1]=r}}function bi(e,t,n){let r=Ln(e,t);return r>=0?e[r|1]=n:(r=~r,zu(e,r,t,n)),r}function _i(e,t){let n=Ln(e,t);if(n>=0)return e[n|1]}function Ln(e,t){return kv(e,t,1)}function kv(e,t,n){let r=0,o=e.length>>n;for(;o!==r;){let i=r+(o-r>>1),s=e[i<<n];if(t===s)return i<<n;s>t?o=i:r=i+1}return~(o<<n)}var St={},pe=[],Or=new g(""),tc=new g("",-1),nc=new g(""),Tr=class{get(t,n=tn){if(n===tn){let o=ju("",-201);throw o.name="\u0275NotFound",o}return n}};function Di(e){return{\u0275providers:e}}function Ei(...e){return{\u0275providers:rc(!0,e),\u0275fromNgModule:!0}}function rc(e,...t){let n=[],r=new Set,o,i=s=>{n.push(s)};return yi(t,s=>{let a=s;li(a,i,[],r)&&(o||=[],o.push(a))}),o!==void 0&&Gu(o,i),n}function Gu(e,t){for(let n=0;n<e.length;n++){let{ngModule:r,providers:o}=e[n];oc(o,i=>{t(i,r)})}}function li(e,t,n,r){if(e=ce(e),!e)return!1;let o=null,i=Ra(e),s=!i&&Tt(e);if(!i&&!s){let c=e.ngModule;if(i=Ra(c),i)o=c;else return!1}else{if(s&&!s.standalone)return!1;o=e}let a=r.has(o);if(s){if(a)return!1;if(r.add(o),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)li(l,t,n,r)}}else if(i){if(i.imports!=null&&!a){r.add(o);let l;yi(i.imports,d=>{li(d,t,n,r)&&(l||=[],l.push(d))}),l!==void 0&&Gu(l,t)}if(!a){let l=rn(o)||(()=>new o);t({provide:o,useFactory:l,deps:pe},o),t({provide:nc,useValue:o,multi:!0},o),t({provide:Or,useValue:()=>M(o),multi:!0},o)}let c=i.providers;if(c!=null&&!a){let l=e;oc(c,d=>{t(d,l)})}}else return!1;return o!==e&&e.providers!==void 0}function oc(e,t){for(let n of e)Ga(n)&&(n=n.\u0275providers),Array.isArray(n)?oc(n,t):t(n)}var Pv=V({provide:String,useValue:V});function Wu(e){return e!==null&&typeof e=="object"&&Pv in e}function Lv(e){return!!(e&&e.useExisting)}function Vv(e){return!!(e&&e.useFactory)}function on(e){return typeof e=="function"}function qu(e){return!!e.useClass}var kr=new g(""),ai={},Fu={},Na;function Vn(){return Na===void 0&&(Na=new Tr),Na}var de=class{},sn=class extends de{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(t,n,r,o){super(),this.parent=n,this.source=r,this.scopes=o,La(t,s=>this.processProvider(s)),this.records.set(tc,Pn(void 0,this)),o.has("environment")&&this.records.set(de,Pn(void 0,this));let i=this.records.get(kr);i!=null&&typeof i.value=="string"&&this.scopes.add(i.value),this.injectorDefTypes=new Set(this.get(nc,pe,{self:!0}))}retrieve(t,n){let r=nn(n)||0;try{return this.get(t,tn,r)}catch(o){if(kn(o))return o;throw o}}destroy(){xr(this),this._destroyed=!0;let t=w(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let n=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of n)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),w(t)}}onDestroy(t){return xr(this),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){xr(this);let n=Qe(this),r=Ce(void 0),o;try{return t()}finally{Qe(n),Ce(r)}}get(t,n=tn,r){if(xr(this),t.hasOwnProperty(Ru))return t[Ru](this);let o=nn(r),i,s=Qe(this),a=Ce(void 0);try{if(!(o&4)){let l=this.records.get(t);if(l===void 0){let d=$v(t)&&hi(t);d&&this.injectableDefInScope(d)?l=Pn(Pa(t),ai):l=null,this.records.set(t,l)}if(l!=null)return this.hydrate(t,l,o)}let c=o&2?Vn():this.parent;return n=o&8&&n===tn?null:n,c.get(t,n)}catch(c){let l=Sv(c);throw l===-200||l===-201?new _(l,null):c}finally{Ce(a),Qe(s)}}resolveInjectorInitializers(){let t=w(null),n=Qe(this),r=Ce(void 0),o;try{let i=this.get(Or,pe,{self:!0});for(let s of i)s()}finally{Qe(n),Ce(r),w(t)}}toString(){return"R3Injector[...]"}processProvider(t){t=ce(t);let n=on(t)?t:ce(t&&t.provide),r=Bv(t);if(!on(t)&&t.multi===!0){let o=this.records.get(n);o||(o=Pn(void 0,ai,!0),o.factory=()=>ka(o.multi),this.records.set(n,o)),n=t,o.multi.push(t)}this.records.set(n,r)}hydrate(t,n,r){let o=w(null);try{if(n.value===Fu)throw Xa("");return n.value===ai&&(n.value=Fu,n.value=n.factory(void 0,r)),typeof n.value=="object"&&n.value&&Uv(n.value)&&this._ngOnDestroyHooks.add(n.value),n.value}finally{w(o)}}injectableDefInScope(t){if(!t.providedIn)return!1;let n=ce(t.providedIn);return typeof n=="string"?n==="any"||this.scopes.has(n):this.injectorDefTypes.has(n)}removeOnDestroy(t){let n=this._onDestroyHooks.indexOf(t);n!==-1&&this._onDestroyHooks.splice(n,1)}};function Pa(e){let t=hi(e),n=t!==null?t.factory:rn(e);if(n!==null)return n;if(e instanceof g)throw new _(-204,!1);if(e instanceof Function)return jv(e);throw new _(-204,!1)}function jv(e){if(e.length>0)throw new _(-204,!1);let n=xv(e);return n!==null?()=>n.factory(e):()=>new e}function Bv(e){if(Wu(e))return Pn(void 0,e.useValue);{let t=ic(e);return Pn(t,ai)}}function ic(e,t,n){let r;if(on(e)){let o=ce(e);return rn(o)||Pa(o)}else if(Wu(e))r=()=>ce(e.useValue);else if(Vv(e))r=()=>e.useFactory(...ka(e.deps||[]));else if(Lv(e))r=(o,i)=>M(ce(e.useExisting),i!==void 0&&i&8?8:void 0);else{let o=ce(e&&(e.useClass||e.provide));if(Hv(e))r=()=>new o(...ka(e.deps));else return rn(o)||Pa(o)}return r}function xr(e){if(e.destroyed)throw new _(-205,!1)}function Pn(e,t,n=!1){return{factory:e,value:t,multi:n?[]:void 0}}function Hv(e){return!!e.deps}function Uv(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function $v(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function La(e,t){for(let n of e)Array.isArray(n)?La(n,t):n&&Ga(n)?La(n.\u0275providers,t):t(n)}function jn(e,t){let n;e instanceof sn?(xr(e),n=e):n=new Oa(e);let r,o=Qe(n),i=Ce(void 0);try{return t()}finally{Qe(o),Ce(i)}}function Zu(){return Bu()!==void 0||si()!=null}var Pe=0,D=1,E=2,ie=3,Te=4,ye=5,Bn=6,Hn=7,fe=8,At=9,Je=10,q=11,Un=12,sc=13,cn=14,Ee=15,Nt=16,ln=17,et=18,Rt=19,ac=20,pt=21,Ci=22,It=23,we=24,dn=25,$n=26,K=27,Yu=1;var Ft=7,Pr=8,un=9,he=10;function ht(e){return Array.isArray(e)&&typeof e[Yu]=="object"}function Le(e){return Array.isArray(e)&&e[Yu]===!0}function cc(e){return(e.flags&4)!==0}function gt(e){return e.componentOffset>-1}function Lr(e){return(e.flags&1)===1}function tt(e){return!!e.template}function zn(e){return(e[E]&512)!==0}function fn(e){return(e[E]&256)===256}var Qu="svg",Ku="math";function Se(e){for(;Array.isArray(e);)e=e[Pe];return e}function lc(e,t){return Se(t[e])}function Ve(e,t){return Se(t[e.index])}function wi(e,t){return e.data[t]}function Xu(e,t){return e[t]}function Ae(e,t){let n=t[e];return ht(n)?n:n[Pe]}function Ju(e){return(e[E]&4)===4}function Ii(e){return(e[E]&128)===128}function ef(e){return Le(e[ie])}function nt(e,t){return t==null?null:e[t]}function dc(e){e[ln]=0}function uc(e){e[E]&1024||(e[E]|=1024,Ii(e)&&mn(e))}function tf(e,t){for(;e>0;)t=t[cn],e--;return t}function Vr(e){return!!(e[E]&9216||e[we]?.dirty)}function xi(e){e[Je].changeDetectionScheduler?.notify(8),e[E]&64&&(e[E]|=1024),Vr(e)&&mn(e)}function mn(e){e[Je].changeDetectionScheduler?.notify(0);let t=xt(e);for(;t!==null&&!(t[E]&8192||(t[E]|=8192,!Ii(t)));)t=xt(t)}function fc(e,t){if(fn(e))throw new _(911,!1);e[pt]===null&&(e[pt]=[]),e[pt].push(t)}function nf(e,t){if(e[pt]===null)return;let n=e[pt].indexOf(t);n!==-1&&e[pt].splice(n,1)}function xt(e){let t=e[ie];return Le(t)?t[ie]:t}function mc(e){return e[Hn]??=[]}function pc(e){return e.cleanup??=[]}function rf(e,t,n,r){let o=mc(t);o.push(n),e.firstCreatePass&&pc(e).push(r,o.length-1)}var x={lFrame:hf(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Va=!1;function of(){return x.lFrame.elementDepthCount}function sf(){x.lFrame.elementDepthCount++}function hc(){x.lFrame.elementDepthCount--}function gc(){return x.bindingsEnabled}function vc(){return x.skipHydrationRootTNode!==null}function yc(e){return x.skipHydrationRootTNode===e}function bc(){x.skipHydrationRootTNode=null}function T(){return x.lFrame.lView}function ne(){return x.lFrame.tView}function Mi(e){return x.lFrame.contextLView=e,e[fe]}function Ti(e){return x.lFrame.contextLView=null,e}function be(){let e=_c();for(;e!==null&&e.type===64;)e=e.parent;return e}function _c(){return x.lFrame.currentTNode}function af(){let e=x.lFrame,t=e.currentTNode;return e.isParent?t:t.parent}function Gn(e,t){let n=x.lFrame;n.currentTNode=e,n.isParent=t}function Dc(){return x.lFrame.isParent}function Ec(){x.lFrame.isParent=!1}function cf(){return x.lFrame.contextLView}function Cc(){return Va}function Sr(e){let t=Va;return Va=e,t}function lf(e){return x.lFrame.bindingIndex=e}function Wn(){return x.lFrame.bindingIndex++}function wc(e){let t=x.lFrame,n=t.bindingIndex;return t.bindingIndex=t.bindingIndex+e,n}function df(){return x.lFrame.inI18n}function uf(e,t){let n=x.lFrame;n.bindingIndex=n.bindingRootIndex=e,Si(t)}function ff(){return x.lFrame.currentDirectiveIndex}function Si(e){x.lFrame.currentDirectiveIndex=e}function mf(e){let t=x.lFrame.currentDirectiveIndex;return t===-1?null:e[t]}function Ai(){return x.lFrame.currentQueryIndex}function jr(e){x.lFrame.currentQueryIndex=e}function zv(e){let t=e[D];return t.type===2?t.declTNode:t.type===1?e[ye]:null}function Ic(e,t,n){if(n&4){let o=t,i=e;for(;o=o.parent,o===null&&!(n&1);)if(o=zv(i),o===null||(i=i[cn],o.type&10))break;if(o===null)return!1;t=o,e=i}let r=x.lFrame=pf();return r.currentTNode=t,r.lView=e,!0}function Ni(e){let t=pf(),n=e[D];x.lFrame=t,t.currentTNode=n.firstChild,t.lView=e,t.tView=n,t.contextLView=e,t.bindingIndex=n.bindingStartIndex,t.inI18n=!1}function pf(){let e=x.lFrame,t=e===null?null:e.child;return t===null?hf(e):t}function hf(e){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=t),t}function gf(){let e=x.lFrame;return x.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var xc=gf;function Ri(){let e=gf();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function vf(e){return(x.lFrame.contextLView=tf(e,x.lFrame.contextLView))[fe]}function Ot(){return x.lFrame.selectedIndex}function kt(e){x.lFrame.selectedIndex=e}function Fi(){let e=x.lFrame;return wi(e.tView,e.selectedIndex)}function yf(){return x.lFrame.currentNamespace}var bf=!0;function Oi(){return bf}function ki(e){bf=e}function ja(e,t=null,n=null,r){let o=_f(e,t,n,r);return o.resolveInjectorInitializers(),o}function _f(e,t=null,n=null,r,o=new Set){let i=[n||pe,Ei(e)],s;return new sn(i,t||Vn(),s||null,o)}var ue=class e{static THROW_IF_NOT_FOUND=tn;static NULL=new Tr;static create(t,n){if(Array.isArray(t))return ja({name:""},n,t,"");{let r=t.name??"";return ja({name:r},t.parent,t.providers,r)}}static \u0275prov=v({token:e,providedIn:"any",factory:()=>M(tc)});static __NG_ELEMENT_ID__=-1},Z=new g(""),Pt=(()=>{class e{static __NG_ELEMENT_ID__=Gv;static __NG_ENV_ID__=n=>n}return e})(),di=class extends Pt{_lView;constructor(t){super(),this._lView=t}get destroyed(){return fn(this._lView)}onDestroy(t){let n=this._lView;return fc(n,t),()=>nf(n,t)}};function Gv(){return new di(T())}var Df=!1,Ef=new g(""),pn=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Xt(!1);debugTaskTracker=p(Ef,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new O(n=>{n.next(!1),n.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let n=this.taskId++;return this.pendingTasks.add(n),this.debugTaskTracker?.add(n),n}has(n){return this.pendingTasks.has(n)}remove(n){this.pendingTasks.delete(n),this.debugTaskTracker?.remove(n),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),Ba=class extends G{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(t=!1){super(),this.__isAsync=t,Zu()&&(this.destroyRef=p(Pt,{optional:!0})??void 0,this.pendingTasks=p(pn,{optional:!0})??void 0)}emit(t){let n=w(null);try{super.next(t)}finally{w(n)}}subscribe(t,n,r){let o=t,i=n||(()=>null),s=r;if(t&&typeof t=="object"){let c=t;o=c.next?.bind(c),i=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(i=this.wrapInTimeout(i),o&&(o=this.wrapInTimeout(o)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:o,error:i,complete:s});return t instanceof te&&t.add(a),a}wrapInTimeout(t){return n=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{t(n)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},le=Ba;function ui(...e){}function Mc(e){let t,n;function r(){e=ui;try{n!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(n),t!==void 0&&clearTimeout(t)}catch{}}return t=setTimeout(()=>{e(),r()}),typeof requestAnimationFrame=="function"&&(n=requestAnimationFrame(()=>{e(),r()})),()=>r()}function Cf(e){return queueMicrotask(()=>e()),()=>{e=ui}}var Tc="isAngularZone",Ar=Tc+"_ID",Wv=0,R=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new le(!1);onMicrotaskEmpty=new le(!1);onStable=new le(!1);onError=new le(!1);constructor(t){let{enableLongStackTrace:n=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:o=!1,scheduleInRootZone:i=Df}=t;if(typeof Zone>"u")throw new _(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),n&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!o&&r,s.shouldCoalesceRunChangeDetection=o,s.callbackScheduled=!1,s.scheduleInRootZone=i,Yv(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Tc)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new _(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new _(909,!1)}run(t,n,r){return this._inner.run(t,n,r)}runTask(t,n,r,o){let i=this._inner,s=i.scheduleEventTask("NgZoneEvent: "+o,t,qv,ui,ui);try{return i.runTask(s,n,r)}finally{i.cancelTask(s)}}runGuarded(t,n,r){return this._inner.runGuarded(t,n,r)}runOutsideAngular(t){return this._outer.run(t)}},qv={};function Sc(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function Zv(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function t(){Mc(()=>{e.callbackScheduled=!1,Ha(e),e.isCheckStableRunning=!0,Sc(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{t()}):e._outer.run(()=>{t()}),Ha(e)}function Yv(e){let t=()=>{Zv(e)},n=Wv++;e._inner=e._inner.fork({name:"angular",properties:{[Tc]:!0,[Ar]:n,[Ar+n]:!0},onInvokeTask:(r,o,i,s,a,c)=>{if(Qv(c))return r.invokeTask(i,s,a,c);try{return Ou(e),r.invokeTask(i,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&t(),ku(e)}},onInvoke:(r,o,i,s,a,c,l)=>{try{return Ou(e),r.invoke(i,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!Kv(c)&&t(),ku(e)}},onHasTask:(r,o,i,s)=>{r.hasTask(i,s),o===i&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,Ha(e),Sc(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,o,i,s)=>(r.handleError(i,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function Ha(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function Ou(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function ku(e){e._nesting--,Sc(e)}var Nr=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new le;onMicrotaskEmpty=new le;onStable=new le;onError=new le;run(t,n,r){return t.apply(n,r)}runGuarded(t,n,r){return t.apply(n,r)}runOutsideAngular(t){return t()}runTask(t,n,r,o){return t.apply(n,r)}};function Qv(e){return wf(e,"__ignore_ng_zone__")}function Kv(e){return wf(e,"__scheduler_tick__")}function wf(e,t){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[t]===!0}var Ke=class{_console=console;handleError(t){this._console.error("ERROR",t)}},Lt=new g("",{factory:()=>{let e=p(R),t=p(de),n;return r=>{e.runOutsideAngular(()=>{t.destroyed&&!n?setTimeout(()=>{throw r}):(n??=t.get(Ke),n.handleError(r))})}}}),If={provide:Or,useValue:()=>{let e=p(Ke,{optional:!0})},multi:!0};function Ne(e,t){let[n,r,o]=sa(e,t?.equal),i=n,s=i[ae];return i.set=r,i.update=o,i.asReadonly=xf.bind(i),i}function xf(){let e=this[ae];if(e.readonlyFn===void 0){let t=()=>this();t[ae]=e,e.readonlyFn=t}return e.readonlyFn}var Br=(()=>{class e{view;node;constructor(n,r){this.view=n,this.node=r}static __NG_ELEMENT_ID__=Xv}return e})();function Xv(){return new Br(T(),be())}var Xe=class{},Hr=new g("",{factory:()=>!0});var Ac=new g(""),Ur=(()=>{class e{internalPendingTasks=p(pn);scheduler=p(Xe);errorHandler=p(Lt);add(){let n=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(n)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(n))}}run(n){let r=this.add();n().catch(this.errorHandler).finally(r)}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),Pi=(()=>{class e{static \u0275prov=v({token:e,providedIn:"root",factory:()=>new Ua})}return e})(),Ua=class{dirtyEffectCount=0;queues=new Map;add(t){this.enqueue(t),this.schedule(t)}schedule(t){t.dirty&&this.dirtyEffectCount++}remove(t){let n=t.zone,r=this.queues.get(n);r.has(t)&&(r.delete(t),t.dirty&&this.dirtyEffectCount--)}enqueue(t){let n=t.zone;this.queues.has(n)||this.queues.set(n,new Set);let r=this.queues.get(n);r.has(t)||r.add(t)}flush(){for(;this.dirtyEffectCount>0;){let t=!1;for(let[n,r]of this.queues)n===null?t||=this.flushQueue(r):t||=n.run(()=>this.flushQueue(r));t||(this.dirtyEffectCount=0)}}flushQueue(t){let n=!1;for(let r of t)r.dirty&&(this.dirtyEffectCount--,n=!0,r.run());return n}},fi=class{[ae];constructor(t){this[ae]=t}destroy(){this[ae].destroy()}};function Nc(e,t){let n=t?.injector??p(ue),r=t?.manualCleanup!==!0?n.get(Pt):null,o,i=n.get(Br,null,{optional:!0}),s=n.get(Xe);return i!==null?(o=ty(i.view,s,e),r instanceof di&&r._lView===i.view&&(r=null)):o=ny(e,n.get(Pi),s),o.injector=n,r!==null&&(o.onDestroyFns=[r.onDestroy(()=>o.destroy())]),new fi(o)}var Mf=N(I({},ca),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=Sr(!1);try{la(this)}finally{Sr(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=w(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],w(e)}}}),Jv=N(I({},Mf),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Et(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),ey=N(I({},Mf),{consumerMarkedDirty(){this.view[E]|=8192,mn(this.view),this.notifier.notify(13)},destroy(){if(Et(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[It]?.delete(this)}});function ty(e,t,n){let r=Object.create(ey);return r.view=e,r.zone=typeof Zone<"u"?Zone.current:null,r.notifier=t,r.fn=Tf(r,n),e[It]??=new Set,e[It].add(r),r.consumerMarkedDirty(r),r}function ny(e,t,n){let r=Object.create(Jv);return r.fn=Tf(r,e),r.scheduler=t,r.notifier=n,r.zone=typeof Zone<"u"?Zone.current:null,r.scheduler.add(r),r.notifier.notify(12),r}function Tf(e,t){return()=>{t(n=>(e.cleanupFns??=[]).push(n))}}function Qr(e){return{toString:e}.toString()}function my(e){return typeof e=="function"}function nm(e,t,n,r){t!==null?t.applyValueToInputSignal(t,r):e[n]=r}var Ui=class{previousValue;currentValue;firstChange;constructor(t,n,r){this.previousValue=t,this.currentValue=n,this.firstChange=r}isFirstChange(){return this.firstChange}},bn=(()=>{let e=()=>rm;return e.ngInherit=!0,e})();function rm(e){return e.type.prototype.ngOnChanges&&(e.setInput=hy),py}function py(){let e=im(this),t=e?.current;if(t){let n=e.previous;if(n===St)e.previous=t;else for(let r in t)n[r]=t[r];e.current=null,this.ngOnChanges(t)}}function hy(e,t,n,r,o){let i=this.declaredInputs[r],s=im(e)||gy(e,{previous:St,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[i];a[i]=new Ui(l&&l.currentValue,n,c===St),nm(e,t,o,n)}var om="__ngSimpleChanges__";function im(e){return e[om]||null}function gy(e,t){return e[om]=t}var Sf=[];var F=function(e,t=null,n){for(let r=0;r<Sf.length;r++){let o=Sf[r];o(e,t,n)}},A=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(A||{});function vy(e,t,n){let{ngOnChanges:r,ngOnInit:o,ngDoCheck:i}=t.type.prototype;if(r){let s=rm(t);(n.preOrderHooks??=[]).push(e,s),(n.preOrderCheckHooks??=[]).push(e,s)}o&&(n.preOrderHooks??=[]).push(0-e,o),i&&((n.preOrderHooks??=[]).push(e,i),(n.preOrderCheckHooks??=[]).push(e,i))}function sm(e,t){for(let n=t.directiveStart,r=t.directiveEnd;n<r;n++){let i=e.data[n].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=i;s&&(e.contentHooks??=[]).push(-n,s),a&&((e.contentHooks??=[]).push(n,a),(e.contentCheckHooks??=[]).push(n,a)),c&&(e.viewHooks??=[]).push(-n,c),l&&((e.viewHooks??=[]).push(n,l),(e.viewCheckHooks??=[]).push(n,l)),d!=null&&(e.destroyHooks??=[]).push(n,d)}}function Vi(e,t,n){am(e,t,3,n)}function ji(e,t,n,r){(e[E]&3)===n&&am(e,t,n,r)}function Rc(e,t){let n=e[E];(n&3)===t&&(n&=16383,n+=1,e[E]=n)}function am(e,t,n,r){let o=r!==void 0?e[ln]&65535:0,i=r??-1,s=t.length-1,a=0;for(let c=o;c<s;c++)if(typeof t[c+1]=="number"){if(a=t[c],r!=null&&a>=r)break}else t[c]<0&&(e[ln]+=65536),(a<i||i==-1)&&(yy(e,n,t,c),e[ln]=(e[ln]&4294901760)+c+2),c++}function Af(e,t){F(A.LifecycleHookStart,e,t);let n=w(null);try{t.call(e)}finally{w(n),F(A.LifecycleHookEnd,e,t)}}function yy(e,t,n,r){let o=n[r]<0,i=n[r+1],s=o?-n[r]:n[r],a=e[s];o?e[E]>>14<e[ln]>>16&&(e[E]&3)===t&&(e[E]+=16384,Af(a,i)):Af(a,i)}var Zn=-1,gn=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(t,n,r,o){this.factory=t,this.name=o,this.canSeeViewProviders=n,this.injectImpl=r}};function by(e){return(e.flags&8)!==0}function _y(e){return(e.flags&16)!==0}function Dy(e,t,n){let r=0;for(;r<n.length;){let o=n[r];if(typeof o=="number"){if(o!==0)break;r++;let i=n[r++],s=n[r++],a=n[r++];e.setAttribute(t,s,a,i)}else{let i=o,s=n[++r];Cy(i)?e.setProperty(t,i,s):e.setAttribute(t,i,s),r++}}return r}function Ey(e){return e===3||e===4||e===6}function Cy(e){return e.charCodeAt(0)===64}function Yn(e,t){if(!(t===null||t.length===0))if(e===null||e.length===0)e=t.slice();else{let n=-1;for(let r=0;r<t.length;r++){let o=t[r];typeof o=="number"?n=o:n===0||(n===-1||n===2?Nf(e,n,o,null,t[++r]):Nf(e,n,o,null,null))}}return e}function Nf(e,t,n,r,o){let i=0,s=e.length;if(t===-1)s=-1;else for(;i<e.length;){let a=e[i++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=i-1;break}}}for(;i<e.length;){let a=e[i];if(typeof a=="number")break;if(a===n){o!==null&&(e[i+1]=o);return}i++,o!==null&&i++}s!==-1&&(e.splice(s,0,t),i=s+1),e.splice(i++,0,n),o!==null&&e.splice(i++,0,o)}function cm(e){return e!==Zn}function $i(e){return e&32767}function wy(e){return e>>16}function zi(e,t){let n=wy(e),r=t;for(;n>0;)r=r[cn],n--;return r}var Uc=!0;function Rf(e){let t=Uc;return Uc=e,t}var Iy=256,lm=Iy-1,dm=5,xy=0,rt={};function My(e,t,n){let r;typeof n=="string"?r=n.charCodeAt(0)||0:n.hasOwnProperty(an)&&(r=n[an]),r==null&&(r=n[an]=xy++);let o=r&lm,i=1<<o;t.data[e+(o>>dm)]|=i}function Gi(e,t){let n=um(e,t);if(n!==-1)return n;let r=t[D];r.firstCreatePass&&(e.injectorIndex=t.length,Fc(r.data,e),Fc(t,null),Fc(r.blueprint,null));let o=pl(e,t),i=e.injectorIndex;if(cm(o)){let s=$i(o),a=zi(o,t),c=a[D].data;for(let l=0;l<8;l++)t[i+l]=a[s+l]|c[s+l]}return t[i+8]=o,i}function Fc(e,t){e.push(0,0,0,0,0,0,0,0,t)}function um(e,t){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||t[e.injectorIndex+8]===null?-1:e.injectorIndex}function pl(e,t){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let n=0,r=null,o=t;for(;o!==null;){if(r=gm(o),r===null)return Zn;if(n++,o=o[cn],r.injectorIndex!==-1)return r.injectorIndex|n<<16}return Zn}function $c(e,t,n){My(e,t,n)}function fm(e,t,n){if(n&8||e!==void 0)return e;vi(t,"NodeInjector")}function mm(e,t,n,r){if(n&8&&r===void 0&&(r=null),(n&3)===0){let o=e[At],i=Ce(void 0);try{return o?o.get(t,r,n&8):Ja(t,r,n&8)}finally{Ce(i)}}return fm(r,t,n)}function pm(e,t,n,r=0,o){if(e!==null){if(t[E]&2048&&!(r&2)){let s=Ny(e,t,n,r,rt);if(s!==rt)return s}let i=hm(e,t,n,r,rt);if(i!==rt)return i}return mm(t,n,r,o)}function hm(e,t,n,r,o){let i=Sy(n);if(typeof i=="function"){if(!Ic(t,e,r))return r&1?fm(o,n,r):mm(t,n,r,o);try{let s;if(s=i(r),s==null&&!(r&8))vi(n);else return s}finally{xc()}}else if(typeof i=="number"){let s=null,a=um(e,t),c=Zn,l=r&1?t[Ee][ye]:null;for((a===-1||r&4)&&(c=a===-1?pl(e,t):t[a+8],c===Zn||!Of(r,!1)?a=-1:(s=t[D],a=$i(c),t=zi(c,t)));a!==-1;){let d=t[D];if(Ff(i,a,d.data)){let u=Ty(a,t,n,s,r,l);if(u!==rt)return u}c=t[a+8],c!==Zn&&Of(r,t[D].data[a+8]===l)&&Ff(i,a,t)?(s=d,a=$i(c),t=zi(c,t)):a=-1}}return o}function Ty(e,t,n,r,o,i){let s=t[D],a=s.data[e+8],c=r==null?gt(a)&&Uc:r!=s&&(a.type&3)!==0,l=o&1&&i===a,d=Bi(a,s,n,c,l);return d!==null?Gr(t,s,d,a,o):rt}function Bi(e,t,n,r,o){let i=e.providerIndexes,s=t.data,a=i&1048575,c=e.directiveStart,l=e.directiveEnd,d=i>>20,u=r?a:a+d,m=o?a+d:l;for(let f=u;f<m;f++){let h=s[f];if(f<c&&n===h||f>=c&&h.type===n)return f}if(o){let f=s[c];if(f&&tt(f)&&f.type===n)return c}return null}function Gr(e,t,n,r,o){let i=e[n],s=t.data;if(i instanceof gn){let a=i;if(a.resolving)throw Xa("");let c=Rf(a.canSeeViewProviders);a.resolving=!0;let l=s[n].type||s[n],d,u=a.injectImpl?Ce(a.injectImpl):null,m=Ic(e,r,0);try{i=e[n]=a.factory(void 0,o,s,e,r),t.firstCreatePass&&n>=r.directiveStart&&vy(n,s[n],t)}finally{u!==null&&Ce(u),Rf(c),a.resolving=!1,xc()}}return i}function Sy(e){if(typeof e=="string")return e.charCodeAt(0)||0;let t=e.hasOwnProperty(an)?e[an]:void 0;return typeof t=="number"?t>=0?t&lm:Ay:t}function Ff(e,t,n){let r=1<<e;return!!(n[t+(e>>dm)]&r)}function Of(e,t){return!(e&2)&&!(e&1&&t)}var hn=class{_tNode;_lView;constructor(t,n){this._tNode=t,this._lView=n}get(t,n,r){return pm(this._tNode,this._lView,t,nn(r),n)}};function Ay(){return new hn(be(),T())}function ts(e){return Qr(()=>{let t=e.prototype.constructor,n=t[Mr]||zc(t),r=Object.prototype,o=Object.getPrototypeOf(e.prototype).constructor;for(;o&&o!==r;){let i=o[Mr]||zc(o);if(i&&i!==n)return i;o=Object.getPrototypeOf(o)}return i=>new i})}function zc(e){return za(e)?()=>{let t=zc(ce(e));return t&&t()}:rn(e)}function Ny(e,t,n,r,o){let i=e,s=t;for(;i!==null&&s!==null&&s[E]&2048&&!zn(s);){let a=hm(i,s,n,r|2,rt);if(a!==rt)return a;let c=i.parent;if(!c){let l=s[ac];if(l){let d=l.get(n,rt,r&-5);if(d!==rt)return d}c=gm(s),s=s[cn]}i=c}return o}function gm(e){let t=e[D],n=t.type;return n===2?t.declTNode:n===1?e[ye]:null}function Ry(){return er(be(),T())}function er(e,t){return new X(Ve(e,t))}var X=(()=>{class e{nativeElement;constructor(n){this.nativeElement=n}static __NG_ELEMENT_ID__=Ry}return e})();function vm(e){return e instanceof X?e.nativeElement:e}function Fy(){return this._results[Symbol.iterator]()}var Wi=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new G}constructor(t=!1){this._emitDistinctChangesOnly=t}get(t){return this._results[t]}map(t){return this._results.map(t)}filter(t){return this._results.filter(t)}find(t){return this._results.find(t)}reduce(t,n){return this._results.reduce(t,n)}forEach(t){this._results.forEach(t)}some(t){return this._results.some(t)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(t,n){this.dirty=!1;let r=Uu(t);(this._changesDetected=!Hu(this._results,r,n))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(t){this._onDirty=t}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Fy};function ym(e){return(e.flags&128)===128}var hl=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(hl||{}),bm=new Map,Oy=0;function ky(){return Oy++}function Py(e){bm.set(e[Rt],e)}function Gc(e){bm.delete(e[Rt])}var kf="__ngContext__";function Qn(e,t){ht(t)?(e[kf]=t[Rt],Py(t)):e[kf]=t}function _m(e){return Em(e[Un])}function Dm(e){return Em(e[Te])}function Em(e){for(;e!==null&&!Le(e);)e=e[Te];return e}var Ly;function gl(e){Ly=e}var tr=new g("",{factory:()=>Vy}),Vy="ng";var ns=new g(""),_n=new g("",{providedIn:"platform",factory:()=>"unknown"}),vl=new g(""),nr=new g("",{factory:()=>p(Z).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Cm=!1,wm=new g("",{factory:()=>Cm});var jy=(e,t,n,r)=>{};function By(e,t,n,r){jy(e,t,n,r)}function rs(e){return(e.flags&32)===32}var Hy=()=>null;function Im(e,t,n=!1){return Hy(e,t,n)}function xm(e,t){let n=e.contentQueries;if(n!==null){let r=w(null);try{for(let o=0;o<n.length;o+=2){let i=n[o],s=n[o+1];if(s!==-1){let a=e.data[s];jr(i),a.contentQueries(2,t[s],s)}}}finally{w(r)}}}function Wc(e,t,n){jr(0);let r=w(null);try{t(e,n)}finally{w(r)}}function Mm(e,t,n){if(cc(t)){let r=w(null);try{let o=t.directiveStart,i=t.directiveEnd;for(let s=o;s<i;s++){let a=e.data[s];if(a.contentQueries){let c=n[s];a.contentQueries(1,c,s)}}}finally{w(r)}}}var He=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(He||{});var qc=class{changingThisBreaksApplicationSecurity;constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${$a})`}};function yl(e){return e instanceof qc?e.changingThisBreaksApplicationSecurity:e}function Uy(e,t){return e.createText(t)}function $y(e,t,n){e.setValue(t,n)}function Tm(e,t,n){return e.createElement(t,n)}function qi(e,t,n,r,o){e.insertBefore(t,n,r,o)}function Sm(e,t,n){e.appendChild(t,n)}function Pf(e,t,n,r,o){r!==null?qi(e,t,n,r,o):Sm(e,t,n)}function zy(e,t,n,r){e.removeChild(null,t,n,r)}function Gy(e,t,n){e.setAttribute(t,"style",n)}function Wy(e,t,n){n===""?e.removeAttribute(t,"class"):e.setAttribute(t,"class",n)}function Am(e,t,n){let{mergedAttrs:r,classes:o,styles:i}=n;r!==null&&Dy(e,t,r),o!==null&&Wy(e,t,o),i!==null&&Gy(e,t,i)}function qy(e,t,n){let r=e.length;for(;;){let o=e.indexOf(t,n);if(o===-1)return o;if(o===0||e.charCodeAt(o-1)<=32){let i=t.length;if(o+i===r||e.charCodeAt(o+i)<=32)return o}n=o+1}}var Nm="ng-template";function Zy(e,t,n,r){let o=0;if(r){for(;o<t.length&&typeof t[o]=="string";o+=2)if(t[o]==="class"&&qy(t[o+1].toLowerCase(),n,0)!==-1)return!0}else if(bl(e))return!1;if(o=t.indexOf(1,o),o>-1){let i;for(;++o<t.length&&typeof(i=t[o])=="string";)if(i.toLowerCase()===n)return!0}return!1}function bl(e){return e.type===4&&e.value!==Nm}function Yy(e,t,n){let r=e.type===4&&!n?Nm:e.value;return t===r}function Qy(e,t,n){let r=4,o=e.attrs,i=o!==null?Jy(o):0,s=!1;for(let a=0;a<t.length;a++){let c=t[a];if(typeof c=="number"){if(!s&&!je(r)&&!je(c))return!1;if(s&&je(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!Yy(e,c,n)||c===""&&t.length===1){if(je(r))return!1;s=!0}}else if(r&8){if(o===null||!Zy(e,o,c,n)){if(je(r))return!1;s=!0}}else{let l=t[++a],d=Ky(c,o,bl(e),n);if(d===-1){if(je(r))return!1;s=!0;continue}if(l!==""){let u;if(d>i?u="":u=o[d+1].toLowerCase(),r&2&&l!==u){if(je(r))return!1;s=!0}}}}return je(r)||s}function je(e){return(e&1)===0}function Ky(e,t,n,r){if(t===null)return-1;let o=0;if(r||!n){let i=!1;for(;o<t.length;){let s=t[o];if(s===e)return o;if(s===3||s===6)i=!0;else if(s===1||s===2){let a=t[++o];for(;typeof a=="string";)a=t[++o];continue}else{if(s===4)break;if(s===0){o+=4;continue}}o+=i?1:2}return-1}else return eb(t,e)}function Rm(e,t,n=!1){for(let r=0;r<t.length;r++)if(Qy(e,t[r],n))return!0;return!1}function Xy(e){let t=e.attrs;if(t!=null){let n=t.indexOf(5);if((n&1)===0)return t[n+1]}return null}function Jy(e){for(let t=0;t<e.length;t++){let n=e[t];if(Ey(n))return t}return e.length}function eb(e,t){let n=e.indexOf(4);if(n>-1)for(n++;n<e.length;){let r=e[n];if(typeof r=="number")return-1;if(r===t)return n;n++}return-1}function tb(e,t){e:for(let n=0;n<t.length;n++){let r=t[n];if(e.length===r.length){for(let o=0;o<e.length;o++)if(e[o]!==r[o])continue e;return!0}}return!1}function Lf(e,t){return e?":not("+t.trim()+")":t}function nb(e){let t=e[0],n=1,r=2,o="",i=!1;for(;n<e.length;){let s=e[n];if(typeof s=="string")if(r&2){let a=e[++n];o+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?o+="."+s:r&4&&(o+=" "+s);else o!==""&&!je(s)&&(t+=Lf(i,o),o=""),r=s,i=i||!je(r);n++}return o!==""&&(t+=Lf(i,o)),t}function rb(e){return e.map(nb).join(",")}function ob(e){let t=[],n=[],r=1,o=2;for(;r<e.length;){let i=e[r];if(typeof i=="string")o===2?i!==""&&t.push(i,e[++r]):o===8&&n.push(i);else{if(!je(o))break;o=i}r++}return n.length&&t.push(1,...n),t}var $e={};function _l(e,t,n,r,o,i,s,a,c,l,d){let u=K+r,m=u+o,f=ib(u,m),h=typeof l=="function"?l():l;return f[D]={type:e,blueprint:f,template:n,queries:null,viewQuery:a,declTNode:t,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof i=="function"?i():i,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:h,incompleteFirstPass:!1,ssrId:d}}function ib(e,t){let n=[];for(let r=0;r<t;r++)n.push(r<e?null:$e);return n}function sb(e){let t=e.tView;return t===null||t.incompleteFirstPass?e.tView=_l(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):t}function Dl(e,t,n,r,o,i,s,a,c,l,d){let u=t.blueprint.slice();return u[Pe]=o,u[E]=r|4|128|8|64|1024,(l!==null||e&&e[E]&2048)&&(u[E]|=2048),dc(u),u[ie]=u[cn]=e,u[fe]=n,u[Je]=s||e&&e[Je],u[q]=a||e&&e[q],u[At]=c||e&&e[At]||null,u[ye]=i,u[Rt]=ky(),u[Bn]=d,u[ac]=l,u[Ee]=t.type==2?e[Ee]:u,u}function ab(e,t,n){let r=Ve(t,e),o=sb(n),i=e[Je].rendererFactory,s=El(e,Dl(e,o,null,Fm(n),r,t,null,i.createRenderer(r,n),null,null,null));return e[t.index]=s}function Fm(e){let t=16;return e.signals?t=4096:e.onPush&&(t=64),t}function Om(e,t,n,r){if(n===0)return-1;let o=t.length;for(let i=0;i<n;i++)t.push(r),e.blueprint.push(r),e.data.push(null);return o}function El(e,t){return e[Un]?e[sc][Te]=t:e[Un]=t,e[sc]=t,t}function Y(e=1){km(ne(),T(),Ot()+e,!1)}function km(e,t,n,r){if(!r)if((t[E]&3)===3){let i=e.preOrderCheckHooks;i!==null&&Vi(t,i,n)}else{let i=e.preOrderHooks;i!==null&&ji(t,i,0,n)}kt(n)}var os=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(os||{});function Zc(e,t,n,r){let o=w(null);try{let[i,s,a]=e.inputs[n],c=null;(s&os.SignalBased)!==0&&(c=t[i][ae]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):a!==null&&(r=a.call(t,r)),e.setInput!==null?e.setInput(t,c,r,n,i):nm(t,c,i,r)}finally{w(o)}}var ot=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(ot||{}),cb;function Cl(e,t){return cb(e,t)}var OA=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Yc=new WeakMap,Qc=new WeakSet;function lb(e,t){let n=Yc.get(e);if(!n||n.length===0)return;let r=t.parentNode,o=t.previousSibling;for(let i=n.length-1;i>=0;i--){let s=n[i],a=s.parentNode;s===t?(n.splice(i,1),Qc.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(o&&s===o||a&&r&&a!==r)&&(n.splice(i,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function db(e,t){let n=Yc.get(e);n?n.includes(t)||n.push(t):Yc.set(e,[t])}var Kn=new Set,is=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(is||{}),it=new g(""),Vf=new Set;function Kr(e){Vf.has(e)||(Vf.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var wl=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),Il=[0,1,2,3],Pm=(()=>{class e{ngZone=p(R);scheduler=p(Xe);errorHandler=p(Ke,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){p(it,{optional:!0})}execute(){let n=this.sequences.size>0;n&&F(A.AfterRenderHooksStart),this.executing=!0;for(let r of Il)for(let o of this.sequences)if(!(o.erroredOrDestroyed||!o.hooks[r]))try{o.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let i=o.hooks[r];return i(o.pipelinedValue)},o.snapshot))}catch(i){o.erroredOrDestroyed=!0,this.errorHandler?.handleError(i)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),n&&F(A.AfterRenderHooksEnd)}register(n){let{view:r}=n;r!==void 0?((r[dn]??=[]).push(n),mn(r),r[E]|=8192):this.executing?this.deferredRegistrations.add(n):this.addSequence(n)}addSequence(n){this.sequences.add(n),this.scheduler.notify(7)}unregister(n){this.executing&&this.sequences.has(n)?(n.erroredOrDestroyed=!0,n.pipelinedValue=void 0,n.once=!0):(this.sequences.delete(n),this.deferredRegistrations.delete(n))}maybeTrace(n,r){return r?r.run(is.AFTER_NEXT_RENDER,n):n()}static \u0275prov=v({token:e,providedIn:"root",factory:()=>new e})}return e})(),Zi=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(t,n,r,o,i,s=null){this.impl=t,this.hooks=n,this.view=r,this.once=o,this.snapshot=s,this.unregisterOnDestroy=i?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let t=this.view?.[dn];t&&(this.view[dn]=t.filter(n=>n!==this))}};var ub=new g("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:p(de)})});function Lm(e,t,n){let r=e.get(ub);if(Array.isArray(t))for(let o of t)r.queue.add(o),n?.detachedLeaveAnimationFns?.push(o);else r.queue.add(t),n?.detachedLeaveAnimationFns?.push(t);r.scheduler&&r.scheduler(e)}function fb(e,t){for(let[n,r]of t)Lm(e,r.animateFns)}function jf(e,t,n,r){let o=e?.[$n]?.enter;t!==null&&o&&o.has(n.index)&&fb(r,o)}function qn(e,t,n,r,o,i,s,a){if(o!=null){let c,l=!1;Le(o)?c=o:ht(o)&&(l=!0,o=o[Pe]);let d=Se(o);e===0&&r!==null?(jf(a,r,i,n),s==null?Sm(t,r,d):qi(t,r,d,s||null,!0)):e===1&&r!==null?(jf(a,r,i,n),qi(t,r,d,s||null,!0),lb(i,d)):e===2?(a?.[$n]?.leave?.has(i.index)&&db(i,d),Bf(a,i,n,u=>{if(Qc.has(d)){Qc.delete(d);return}zy(t,d,l,u)})):e===3&&Bf(a,i,n,()=>{t.destroyNode(d)}),c!=null&&Cb(t,e,n,c,i,r,s)}}function mb(e,t){Vm(e,t),t[Pe]=null,t[ye]=null}function pb(e,t,n,r,o,i){r[Pe]=o,r[ye]=t,ss(e,r,n,1,o,i)}function Vm(e,t){t[Je].changeDetectionScheduler?.notify(9),ss(e,t,t[q],2,null,null)}function hb(e){let t=e[Un];if(!t)return Oc(e[D],e);for(;t;){let n=null;if(ht(t))n=t[Un];else{let r=t[he];r&&(n=r)}if(!n){for(;t&&!t[Te]&&t!==e;)ht(t)&&Oc(t[D],t),t=t[ie];t===null&&(t=e),ht(t)&&Oc(t[D],t),n=t&&t[Te]}t=n}}function xl(e,t){let n=e[un],r=n.indexOf(t);n.splice(r,1)}function Ml(e,t){if(fn(t))return;let n=t[q];n.destroyNode&&ss(e,t,n,3,null,null),hb(t)}function Oc(e,t){if(fn(t))return;let n=w(null);try{t[E]&=-129,t[E]|=256,t[we]&&Et(t[we]),yb(e,t),vb(e,t),t[D].type===1&&t[q].destroy();let r=t[Nt];if(r!==null&&Le(t[ie])){r!==t[ie]&&xl(r,t);let o=t[et];o!==null&&o.detachView(e)}Gc(t)}finally{w(n)}}function Bf(e,t,n,r){let o=e?.[$n];if(o==null||o.leave==null||!o.leave.has(t.index))return r(!1);e&&Kn.add(e[Rt]),Lm(n,()=>{if(o.leave&&o.leave.has(t.index)){let s=o.leave.get(t.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}o.detachedLeaveAnimationFns=void 0}o.running=Promise.allSettled(a),gb(e,r)}else e&&Kn.delete(e[Rt]),r(!1)},o)}function gb(e,t){let n=e[$n]?.running;if(n){n.then(()=>{e[$n].running=void 0,Kn.delete(e[Rt]),t(!0)});return}t(!1)}function vb(e,t){let n=e.cleanup,r=t[Hn];if(n!==null)for(let s=0;s<n.length-1;s+=2)if(typeof n[s]=="string"){let a=n[s+3];a>=0?r[a]():r[-a].unsubscribe(),s+=2}else{let a=r[n[s+1]];n[s].call(a)}r!==null&&(t[Hn]=null);let o=t[pt];if(o!==null){t[pt]=null;for(let s=0;s<o.length;s++){let a=o[s];a()}}let i=t[It];if(i!==null){t[It]=null;for(let s of i)s.destroy()}}function yb(e,t){let n;if(e!=null&&(n=e.destroyHooks)!=null)for(let r=0;r<n.length;r+=2){let o=t[n[r]];if(!(o instanceof gn)){let i=n[r+1];if(Array.isArray(i))for(let s=0;s<i.length;s+=2){let a=o[i[s]],c=i[s+1];F(A.LifecycleHookStart,a,c);try{c.call(a)}finally{F(A.LifecycleHookEnd,a,c)}}else{F(A.LifecycleHookStart,o,i);try{i.call(o)}finally{F(A.LifecycleHookEnd,o,i)}}}}}function jm(e,t,n){return bb(e,t.parent,n)}function bb(e,t,n){let r=t;for(;r!==null&&r.type&168;)t=r,r=t.parent;if(r===null)return n[Pe];if(gt(r)){let{encapsulation:o}=e.data[r.directiveStart+r.componentOffset];if(o===He.None||o===He.Emulated)return null}return Ve(r,n)}function Bm(e,t,n){return Db(e,t,n)}function _b(e,t,n){return e.type&40?Ve(e,n):null}var Db=_b,Hf;function Tl(e,t,n,r){let o=jm(e,r,t),i=t[q],s=r.parent||t[ye],a=Bm(s,r,t);if(o!=null)if(Array.isArray(n))for(let c=0;c<n.length;c++)Pf(i,o,n[c],a,!1);else Pf(i,o,n,a,!1);Hf!==void 0&&Hf(i,r,t,n,o)}function $r(e,t){if(t!==null){let n=t.type;if(n&3)return Ve(t,e);if(n&4)return Kc(-1,e[t.index]);if(n&8){let r=t.child;if(r!==null)return $r(e,r);{let o=e[t.index];return Le(o)?Kc(-1,o):Se(o)}}else{if(n&128)return $r(e,t.next);if(n&32)return Cl(t,e)()||Se(e[t.index]);{let r=Hm(e,t);if(r!==null){if(Array.isArray(r))return r[0];let o=xt(e[Ee]);return $r(o,r)}else return $r(e,t.next)}}}return null}function Hm(e,t){if(t!==null){let r=e[Ee][ye],o=t.projection;return r.projection[o]}return null}function Kc(e,t){let n=he+e+1;if(n<t.length){let r=t[n],o=r[D].firstChild;if(o!==null)return $r(r,o)}return t[Ft]}function Sl(e,t,n,r,o,i,s){for(;n!=null;){let a=r[At];if(n.type===128){n=n.next;continue}let c=r[n.index],l=n.type;if(s&&t===0&&(c&&Qn(Se(c),r),n.flags|=2),!rs(n))if(l&8)Sl(e,t,n.child,r,o,i,!1),qn(t,e,a,o,c,n,i,r);else if(l&32){let d=Cl(n,r),u;for(;u=d();)qn(t,e,a,o,u,n,i,r);qn(t,e,a,o,c,n,i,r)}else l&16?Um(e,t,r,n,o,i):qn(t,e,a,o,c,n,i,r);n=s?n.projectionNext:n.next}}function ss(e,t,n,r,o,i){Sl(n,r,e.firstChild,t,o,i,!1)}function Eb(e,t,n){let r=t[q],o=jm(e,n,t),i=n.parent||t[ye],s=Bm(i,n,t);Um(r,0,t,n,o,s)}function Um(e,t,n,r,o,i){let s=n[Ee],c=s[ye].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];qn(t,e,n[At],o,d,r,i,n)}else{let l=c,d=s[ie];ym(r)&&(l.flags|=128),Sl(e,t,l,d,o,i,!0)}}function Cb(e,t,n,r,o,i,s){let a=r[Ft],c=Se(r);a!==c&&qn(t,e,n,i,a,o,s);for(let l=he;l<r.length;l++){let d=r[l];ss(d[D],d,e,t,i,a)}}function wb(e,t,n,r,o){if(t)o?e.addClass(n,r):e.removeClass(n,r);else{let i=r.indexOf("-")===-1?void 0:ot.DashCase;o==null?e.removeStyle(n,r,i):(typeof o=="string"&&o.endsWith("!important")&&(o=o.slice(0,-10),i|=ot.Important),e.setStyle(n,r,o,i))}}function $m(e,t,n,r,o){let i=Ot(),s=r&2;try{kt(-1),s&&t.length>K&&km(e,t,K,!1);let a=s?A.TemplateUpdateStart:A.TemplateCreateStart;F(a,o,n),n(r,o)}finally{kt(i);let a=s?A.TemplateUpdateEnd:A.TemplateCreateEnd;F(a,o,n)}}function Al(e,t,n){Nb(e,t,n),(n.flags&64)===64&&Rb(e,t,n)}function as(e,t,n=Ve){let r=t.localNames;if(r!==null){let o=t.index+1;for(let i=0;i<r.length;i+=2){let s=r[i+1],a=s===-1?n(t,e):e[s];e[o++]=a}}}function Ib(e,t,n,r){let i=r.get(wm,Cm)||n===He.ShadowDom||n===He.ExperimentalIsolatedShadowDom,s=e.selectRootElement(t,i);return xb(s),s}function xb(e){Mb(e)}var Mb=()=>null;function Tb(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function Sb(e,t,n,r,o,i){let s=t[D];if(Nl(e,s,t,n,r)){gt(e)&&Ab(t,e.index);return}e.type&3&&(n=Tb(n)),zm(e,t,n,r,o,i)}function zm(e,t,n,r,o,i){if(e.type&3){let s=Ve(e,t);r=i!=null?i(r,e.value||"",n):r,o.setProperty(s,n,r)}else e.type&12}function Ab(e,t){let n=Ae(t,e);n[E]&16||(n[E]|=64)}function Nb(e,t,n){let r=n.directiveStart,o=n.directiveEnd;gt(n)&&ab(t,n,e.data[r+n.componentOffset]),e.firstCreatePass||Gi(n,t);let i=n.initialInputs;for(let s=r;s<o;s++){let a=e.data[s],c=Gr(t,e,s,n);if(Qn(c,t),i!==null&&Pb(t,s-r,c,a,n,i),tt(a)){let l=Ae(n.index,t);l[fe]=Gr(t,e,s,n)}}}function Rb(e,t,n){let r=n.directiveStart,o=n.directiveEnd,i=n.index,s=ff();try{kt(i);for(let a=r;a<o;a++){let c=e.data[a],l=t[a];Si(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&Fb(c,l)}}finally{kt(-1),Si(s)}}function Fb(e,t){e.hostBindings!==null&&e.hostBindings(1,t)}function Gm(e,t){let n=e.directiveRegistry,r=null;if(n)for(let o=0;o<n.length;o++){let i=n[o];Rm(t,i.selectors,!1)&&(r??=[],tt(i)?r.unshift(i):r.push(i))}return r}function Ob(e,t,n,r,o,i){let s=Ve(e,t);kb(t[q],s,i,e.value,n,r,o)}function kb(e,t,n,r,o,i,s){if(i==null)e.removeAttribute(t,o,n);else{let a=s==null?Ka(i):s(i,r||"",o);e.setAttribute(t,o,a,n)}}function Pb(e,t,n,r,o,i){let s=i[t];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Zc(r,n,c,l)}}function Wm(e,t,n,r,o){let i=K+n,s=t[D],a=o(s,t,e,r,n);t[i]=a,Gn(e,!0);let c=e.type===2;return c?(Am(t[q],a,e),(of()===0||Lr(e))&&Qn(a,t),sf()):Qn(a,t),Oi()&&(!c||!rs(e))&&Tl(s,t,a,e),e}function qm(e){let t=e;return Dc()?Ec():(t=t.parent,Gn(t,!1)),t}function Lb(e,t){let n=e[At];if(!n)return;let r;try{r=n.get(Lt,null)}catch{r=null}r?.(t)}function Nl(e,t,n,r,o){let i=e.inputs?.[r],s=e.hostDirectiveInputs?.[r],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=t.data[l];Zc(u,n[l],d,o),a=!0}if(i)for(let c of i){let l=n[c],d=t.data[c];Zc(d,l,r,o),a=!0}return a}function Vb(e,t){let n=Ae(t,e),r=n[D];jb(r,n);let o=n[Pe];o!==null&&n[Bn]===null&&(n[Bn]=Im(o,n[At])),F(A.ComponentStart);try{Rl(r,n,n[fe])}finally{F(A.ComponentEnd,n[fe])}}function jb(e,t){for(let n=t.length;n<e.blueprint.length;n++)t.push(e.blueprint[n])}function Rl(e,t,n){Ni(t);try{let r=e.viewQuery;r!==null&&Wc(1,r,n);let o=e.template;o!==null&&$m(e,t,o,1,n),e.firstCreatePass&&(e.firstCreatePass=!1),t[et]?.finishViewCreation(e),e.staticContentQueries&&xm(e,t),e.staticViewQueries&&Wc(2,e.viewQuery,n);let i=e.components;i!==null&&Bb(t,i)}catch(r){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),r}finally{t[E]&=-5,Ri()}}function Bb(e,t){for(let n=0;n<t.length;n++)Vb(e,t[n])}function Fl(e,t,n,r){let o=w(null);try{let i=t.tView,a=e[E]&4096?4096:16,c=Dl(e,i,n,a,null,t,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),l=e[t.index];c[Nt]=l;let d=e[et];return d!==null&&(c[et]=d.createEmbeddedView(i)),Rl(i,c,n),c}finally{w(o)}}function Yi(e,t){return!t||t.firstChild===null||ym(e)}function Wr(e,t,n,r,o=!1){for(;n!==null;){if(n.type===128){n=o?n.projectionNext:n.next;continue}let i=t[n.index];i!==null&&r.push(Se(i)),Le(i)&&Zm(i,r);let s=n.type;if(s&8)Wr(e,t,n.child,r);else if(s&32){let a=Cl(n,t),c;for(;c=a();)r.push(c)}else if(s&16){let a=Hm(t,n);if(Array.isArray(a))r.push(...a);else{let c=xt(t[Ee]);Wr(c[D],c,a,r,!0)}}n=o?n.projectionNext:n.next}return r}function Zm(e,t){for(let n=he;n<e.length;n++){let r=e[n],o=r[D].firstChild;o!==null&&Wr(r[D],r,o,t)}e[Ft]!==e[Pe]&&t.push(e[Ft])}function Ym(e){if(e[dn]!==null){for(let t of e[dn])t.impl.addSequence(t);e[dn].length=0}}var Qm=[];function Hb(e){return e[we]??Ub(e)}function Ub(e){let t=Qm.pop()??Object.create(zb);return t.lView=e,t}function $b(e){e.lView[we]!==e&&(e.lView=null,Qm.push(e))}var zb=N(I({},Wt),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{mn(e.lView)},consumerOnSignalRead(){this.lView[we]=this}});function Gb(e){let t=e[we]??Object.create(Wb);return t.lView=e,t}var Wb=N(I({},Wt),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let t=xt(e.lView);for(;t&&!Km(t[D]);)t=xt(t);t&&uc(t)},consumerOnSignalRead(){this.lView[we]=this}});function Km(e){return e.type!==2}function Xm(e){if(e[It]===null)return;let t=!0;for(;t;){let n=!1;for(let r of e[It])r.dirty&&(n=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()));t=n&&!!(e[E]&8192)}}var qb=100;function Jm(e,t=0){let r=e[Je].rendererFactory,o=!1;o||r.begin?.();try{Zb(e,t)}finally{o||r.end?.()}}function Zb(e,t){let n=Cc();try{Sr(!0),Xc(e,t);let r=0;for(;Vr(e);){if(r===qb)throw new _(103,!1);r++,Xc(e,1)}}finally{Sr(n)}}function Yb(e,t,n,r){if(fn(t))return;let o=t[E],i=!1,s=!1;Ni(t);let a=!0,c=null,l=null;i||(Km(e)?(l=Hb(t),c=Dt(l)):So()===null?(a=!1,l=Gb(t),c=Dt(l)):t[we]&&(Et(t[we]),t[we]=null));try{dc(t),lf(e.bindingStartIndex),n!==null&&$m(e,t,n,2,r);let d=(o&3)===3;if(!i)if(d){let f=e.preOrderCheckHooks;f!==null&&Vi(t,f,null)}else{let f=e.preOrderHooks;f!==null&&ji(t,f,0,null),Rc(t,0)}if(s||Qb(t),Xm(t),ep(t,0),e.contentQueries!==null&&xm(e,t),!i)if(d){let f=e.contentCheckHooks;f!==null&&Vi(t,f)}else{let f=e.contentHooks;f!==null&&ji(t,f,1),Rc(t,1)}Xb(e,t);let u=e.components;u!==null&&np(t,u,0);let m=e.viewQuery;if(m!==null&&Wc(2,m,r),!i)if(d){let f=e.viewCheckHooks;f!==null&&Vi(t,f)}else{let f=e.viewHooks;f!==null&&ji(t,f,2),Rc(t,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),t[Ci]){for(let f of t[Ci])f();t[Ci]=null}i||(Ym(t),t[E]&=-73)}catch(d){throw i||mn(t),d}finally{l!==null&&(qt(l,c),a&&$b(l)),Ri()}}function ep(e,t){for(let n=_m(e);n!==null;n=Dm(n))for(let r=he;r<n.length;r++){let o=n[r];tp(o,t)}}function Qb(e){for(let t=_m(e);t!==null;t=Dm(t)){if(!(t[E]&2))continue;let n=t[un];for(let r=0;r<n.length;r++){let o=n[r];uc(o)}}}function Kb(e,t,n){F(A.ComponentStart);let r=Ae(t,e);try{tp(r,n)}finally{F(A.ComponentEnd,r[fe])}}function tp(e,t){Ii(e)&&Xc(e,t)}function Xc(e,t){let r=e[D],o=e[E],i=e[we],s=!!(t===0&&o&16);if(s||=!!(o&64&&t===0),s||=!!(o&1024),s||=!!(i?.dirty&&Tn(i)),s||=!1,i&&(i.dirty=!1),e[E]&=-9217,s)Yb(r,e,r.template,e[fe]);else if(o&8192){let a=w(null);try{Xm(e),ep(e,1);let c=r.components;c!==null&&np(e,c,1),Ym(e)}finally{w(a)}}}function np(e,t,n){for(let r=0;r<t.length;r++)Kb(e,t[r],n)}function Xb(e,t){let n=e.hostBindingOpCodes;if(n!==null)try{for(let r=0;r<n.length;r++){let o=n[r];if(o<0)kt(~o);else{let i=o,s=n[++r],a=n[++r];uf(s,i);let c=t[i];F(A.HostBindingsUpdateStart,c);try{a(2,c)}finally{F(A.HostBindingsUpdateEnd,c)}}}}finally{kt(-1)}}function Ol(e,t){let n=Cc()?64:1088;for(e[Je].changeDetectionScheduler?.notify(t);e;){e[E]|=n;let r=xt(e);if(zn(e)&&!r)return e;e=r}return null}function rp(e,t,n,r){return[e,!0,0,t,null,r,null,n,null,null]}function Jb(e,t){let n=he+t;if(n<e.length)return e[n]}function kl(e,t,n,r=!0){let o=t[D];if(t_(o,t,e,n),r){let s=Kc(n,e),a=t[q],c=a.parentNode(e[Ft]);c!==null&&pb(o,e[ye],a,t,c,s)}let i=t[Bn];i!==null&&i.firstChild!==null&&(i.firstChild=null)}function e_(e,t){let n=Qi(e,t);return n!==void 0&&Ml(n[D],n),n}function Qi(e,t){if(e.length<=he)return;let n=he+t,r=e[n];if(r){let o=r[Nt];o!==null&&o!==e&&xl(o,r),t>0&&(e[n-1][Te]=r[Te]);let i=Fr(e,he+t);mb(r[D],r);let s=i[et];s!==null&&s.detachView(i[D]),r[ie]=null,r[Te]=null,r[E]&=-129}return r}function t_(e,t,n,r){let o=he+r,i=n.length;r>0&&(n[o-1][Te]=t),r<i-he?(t[Te]=n[o],ec(n,he+r,t)):(n.push(t),t[Te]=null),t[ie]=n;let s=t[Nt];s!==null&&n!==s&&op(s,t);let a=t[et];a!==null&&a.insertView(e),xi(t),t[E]|=128}function op(e,t){let n=e[un],r=t[ie];if(ht(r))e[E]|=2;else{let o=r[ie][Ee];t[Ee]!==o&&(e[E]|=2)}n===null?e[un]=[t]:n.push(t)}var Vt=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let t=this._lView,n=t[D];return Wr(n,t,n.firstChild,[])}constructor(t,n){this._lView=t,this._cdRefInjectingView=n}get context(){return this._lView[fe]}set context(t){this._lView[fe]=t}get destroyed(){return fn(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[ie];if(Le(t)){let n=t[Pr],r=n?n.indexOf(this):-1;r>-1&&(Qi(t,r),Fr(n,r))}this._attachedToViewContainer=!1}Ml(this._lView[D],this._lView)}onDestroy(t){fc(this._lView,t)}markForCheck(){Ol(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[E]&=-129}reattach(){xi(this._lView),this._lView[E]|=128}detectChanges(){this._lView[E]|=1024,Jm(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new _(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=zn(this._lView),n=this._lView[Nt];n!==null&&!t&&xl(n,this._lView),Vm(this._lView[D],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new _(902,!1);this._appRef=t;let n=zn(this._lView),r=this._lView[Nt];r!==null&&!n&&op(r,this._lView),xi(this._lView)}};var vn=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=n_;constructor(n,r,o){this._declarationLView=n,this._declarationTContainer=r,this.elementRef=o}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(n,r){return this.createEmbeddedViewImpl(n,r)}createEmbeddedViewImpl(n,r,o){let i=Fl(this._declarationLView,this._declarationTContainer,n,{embeddedViewInjector:r,dehydratedView:o});return new Vt(i)}}return e})();function n_(){return cs(be(),T())}function cs(e,t){return e.type&4?new vn(t,e,er(e,t)):null}function rr(e,t,n,r,o){let i=e.data[t];if(i===null)i=r_(e,t,n,r,o),df()&&(i.flags|=32);else if(i.type&64){i.type=n,i.value=r,i.attrs=o;let s=af();i.injectorIndex=s===null?-1:s.injectorIndex}return Gn(i,!0),i}function r_(e,t,n,r,o){let i=_c(),s=Dc(),a=s?i:i&&i.parent,c=e.data[t]=i_(e,a,n,t,r,o);return o_(e,c,i,s),c}function o_(e,t,n,r){e.firstChild===null&&(e.firstChild=t),n!==null&&(r?n.child==null&&t.parent!==null&&(n.child=t):n.next===null&&(n.next=t,t.prev=n))}function i_(e,t,n,r,o,i){let s=t?t.injectorIndex:-1,a=0;return vc()&&(a|=128),{type:n,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:o,attrs:i,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var s_=()=>null,a_=()=>null;function Jc(e,t){return s_(e,t)}function c_(e,t,n){return a_(e,t,n)}var ip=class{},ls=class{},el=class{resolveComponentFactory(t){throw new _(917,!1)}},ds=class{static NULL=new el},Ue=class{},vt=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>l_()}return e})();function l_(){let e=T(),t=be(),n=Ae(t.index,e);return(ht(n)?n:e)[q]}var sp=(()=>{class e{static \u0275prov=v({token:e,providedIn:"root",factory:()=>null})}return e})();var Hi={},tl=class{injector;parentInjector;constructor(t,n){this.injector=t,this.parentInjector=n}get(t,n,r){let o=this.injector.get(t,Hi,r);return o!==Hi||n===Hi?o:this.parentInjector.get(t,n,r)}};function Ki(e,t,n){let r=n?e.styles:null,o=n?e.classes:null,i=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")i=a;else if(i==1)o=pi(o,a);else if(i==2){let c=a,l=t[++s];r=pi(r,c+": "+l+";")}}n?e.styles=r:e.stylesWithoutHost=r,n?e.classes=o:e.classesWithoutHost=o}function j(e,t=0){let n=T();if(n===null)return M(e,t);let r=be();return pm(r,n,ce(e),t)}function ap(e,t,n,r,o){let i=r===null?null:{"":-1},s=o(e,n);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}f_(e,t,n,a,i,c,l)}i!==null&&r!==null&&d_(n,r,i)}function d_(e,t,n){let r=e.localNames=[];for(let o=0;o<t.length;o+=2){let i=n[t[o+1]];if(i==null)throw new _(-301,!1);r.push(t[o],i)}}function u_(e,t,n){t.componentOffset=n,(e.components??=[]).push(t.index)}function f_(e,t,n,r,o,i,s){let a=r.length,c=null;for(let m=0;m<a;m++){let f=r[m];c===null&&tt(f)&&(c=f,u_(e,n,m)),$c(Gi(n,t),e,f.type)}y_(n,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let f=r[m];f.providersResolver&&f.providersResolver(f)}let l=!1,d=!1,u=Om(e,t,a,null);a>0&&(n.directiveToIndex=new Map);for(let m=0;m<a;m++){let f=r[m];if(n.mergedAttrs=Yn(n.mergedAttrs,f.hostAttrs),p_(e,n,t,u,f),v_(u,f,o),s!==null&&s.has(f)){let[C,y]=s.get(f);n.directiveToIndex.set(f.type,[u,C+n.directiveStart,y+n.directiveStart])}else(i===null||!i.has(f))&&n.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(n.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(n.flags|=64);let h=f.type.prototype;!l&&(h.ngOnChanges||h.ngOnInit||h.ngDoCheck)&&((e.preOrderHooks??=[]).push(n.index),l=!0),!d&&(h.ngOnChanges||h.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(n.index),d=!0),u++}m_(e,n,i)}function m_(e,t,n){for(let r=t.directiveStart;r<t.directiveEnd;r++){let o=e.data[r];if(n===null||!n.has(o))Uf(0,t,o,r),Uf(1,t,o,r),zf(t,r,!1);else{let i=n.get(o);$f(0,t,i,r),$f(1,t,i,r),zf(t,r,!0)}}}function Uf(e,t,n,r){let o=e===0?n.inputs:n.outputs;for(let i in o)if(o.hasOwnProperty(i)){let s;e===0?s=t.inputs??={}:s=t.outputs??={},s[i]??=[],s[i].push(r),cp(t,i)}}function $f(e,t,n,r){let o=e===0?n.inputs:n.outputs;for(let i in o)if(o.hasOwnProperty(i)){let s=o[i],a;e===0?a=t.hostDirectiveInputs??={}:a=t.hostDirectiveOutputs??={},a[s]??=[],a[s].push(r,i),cp(t,s)}}function cp(e,t){t==="class"?e.flags|=8:t==="style"&&(e.flags|=16)}function zf(e,t,n){let{attrs:r,inputs:o,hostDirectiveInputs:i}=e;if(r===null||!n&&o===null||n&&i===null||bl(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<r.length;){let c=r[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!n&&o.hasOwnProperty(c)){let l=o[c];for(let d of l)if(d===t){s??=[],s.push(c,r[a+1]);break}}else if(n&&i.hasOwnProperty(c)){let l=i[c];for(let d=0;d<l.length;d+=2)if(l[d]===t){s??=[],s.push(l[d+1],r[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function p_(e,t,n,r,o){e.data[r]=o;let i=o.factory||(o.factory=rn(o.type,!0)),s=new gn(i,tt(o),j,null);e.blueprint[r]=s,n[r]=s,h_(e,t,r,Om(e,n,o.hostVars,$e),o)}function h_(e,t,n,r,o){let i=o.hostBindings;if(i){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~t.index;g_(s)!=a&&s.push(a),s.push(n,r,i)}}function g_(e){let t=e.length;for(;t>0;){let n=e[--t];if(typeof n=="number"&&n<0)return n}return 0}function v_(e,t,n){if(n){if(t.exportAs)for(let r=0;r<t.exportAs.length;r++)n[t.exportAs[r]]=e;tt(t)&&(n[""]=e)}}function y_(e,t,n){e.flags|=1,e.directiveStart=t,e.directiveEnd=t+n,e.providerIndexes=t}function lp(e,t,n,r,o,i,s,a){let c=t[D],l=c.consts,d=nt(l,s),u=rr(c,e,n,r,d);return i&&ap(c,t,u,nt(l,a),o),u.mergedAttrs=Yn(u.mergedAttrs,u.attrs),u.attrs!==null&&Ki(u,u.attrs,!1),u.mergedAttrs!==null&&Ki(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function dp(e,t){sm(e,t),cc(t)&&e.queries.elementEnd(t)}function b_(e,t,n,r,o,i){let s=t.consts,a=nt(s,o),c=rr(t,e,n,r,a);if(c.mergedAttrs=Yn(c.mergedAttrs,c.attrs),i!=null){let l=nt(s,i);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Ki(c,c.attrs,!1),c.mergedAttrs!==null&&Ki(c,c.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,c),c}function Pl(e){return fp(e)?Array.isArray(e)||!(e instanceof Map)&&Symbol.iterator in e:!1}function up(e,t){if(Array.isArray(e))for(let n=0;n<e.length;n++)t(e[n]);else{let n=e[Symbol.iterator](),r;for(;!(r=n.next()).done;)t(r.value)}}function fp(e){return e!==null&&(typeof e=="function"||typeof e=="object")}function Dn(e,t,n){if(n===$e)return!1;let r=e[t];return Object.is(r,n)?!1:(e[t]=n,!0)}function kc(e,t,n){return function r(o){let i=gt(e)?Ae(e.index,t):t;Ol(i,5);let s=t[fe],a=Gf(t,s,n,o),c=r.__ngNextListenerFn__;for(;c;)a=Gf(t,s,c,o)&&a,c=c.__ngNextListenerFn__;return a}}function Gf(e,t,n,r){let o=w(null);try{return F(A.OutputStart,t,n),n(r)!==!1}catch(i){return Lb(e,i),!1}finally{F(A.OutputEnd,t,n),w(o)}}function __(e,t,n,r,o,i,s,a){let c=Lr(e),l=!1,d=null;if(!r&&c&&(d=E_(t,n,i,e.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=Ve(e,n),m=r?r(u):u;By(n,m,i,a);let f=o.listen(m,i,a);if(!D_(i)){let h=r?C=>r(Se(C[e.index])):e.index;mp(h,t,n,i,a,f,!1)}}return l}function D_(e){return e.startsWith("animation")||e.startsWith("transition")}function E_(e,t,n,r){let o=e.cleanup;if(o!=null)for(let i=0;i<o.length-1;i+=2){let s=o[i];if(s===n&&o[i+1]===r){let a=t[Hn],c=o[i+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(i+=2)}return null}function mp(e,t,n,r,o,i,s){let a=t.firstCreatePass?pc(t):null,c=mc(n),l=c.length;c.push(o,i),a&&a.push(r,e,l,(l+1)*(s?-1:1))}function Wf(e,t,n,r,o,i){let s=t[n],a=t[D],l=a.data[n].outputs[r],u=s[l].subscribe(i);mp(e.index,a,t,o,i,u,!0)}var nl=Symbol("BINDING");function pp(e){return e.debugInfo?.className||e.type.name||null}var rl=class extends ds{ngModule;constructor(t){super(),this.ngModule=t}resolveComponentFactory(t){let n=Tt(t);return new Xn(n,this.ngModule)}};function C_(e){return Object.keys(e).map(t=>{let[n,r,o]=e[t],i={propName:n,templateName:t,isSignal:(r&os.SignalBased)!==0};return o&&(i.transform=o),i})}function w_(e){return Object.keys(e).map(t=>({propName:e[t],templateName:t}))}function I_(e,t,n){let r=t instanceof de?t:t?.injector;return r&&e.getStandaloneInjector!==null&&(r=e.getStandaloneInjector(r)||r),r?new tl(n,r):n}function x_(e){let t=e.get(Ue,null);if(t===null)throw new _(407,!1);let n=e.get(sp,null),r=e.get(Xe,null),o=e.get(it,null,{optional:!0});return{rendererFactory:t,sanitizer:n,changeDetectionScheduler:r,ngReflect:!1,tracingService:o}}function M_(e,t){let n=hp(e);return Tm(t,n,n==="svg"?Qu:n==="math"?Ku:null)}function hp(e){return(e.selectors[0][0]||"div").toLowerCase()}var Xn=class extends ls{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=C_(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=w_(this.componentDef.outputs),this.cachedOutputs}constructor(t,n){super(),this.componentDef=t,this.ngModule=n,this.componentType=t.type,this.selector=rb(t.selectors),this.ngContentSelectors=t.ngContentSelectors??[],this.isBoundToModule=!!n}create(t,n,r,o,i,s){F(A.DynamicComponentStart);let a=w(null);try{let c=this.componentDef,l=I_(c,o||this.ngModule,t),d=x_(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(pp(c),()=>this.createComponentRef(d,l,n,r,i,s)):this.createComponentRef(d,l,n,r,i,s)}finally{w(a)}}createComponentRef(t,n,r,o,i,s){let a=this.componentDef,c=T_(o,a,s,i),l=t.rendererFactory.createRenderer(null,a),d=o?Ib(l,o,a.encapsulation,n):M_(a,l),u=s?.some(qf)||i?.some(h=>typeof h!="function"&&h.bindings.some(qf)),m=Dl(null,c,null,512|Fm(a),null,null,t,l,n,null,Im(d,n,!0));m[K]=d,Ni(m);let f=null;try{let h=lp(K,m,2,"#host",()=>c.directiveRegistry,!0,0);Am(l,d,h),Qn(d,m),Al(c,m,h),Mm(c,h,m),dp(c,h),r!==void 0&&A_(h,this.ngContentSelectors,r),f=Ae(h.index,m),m[fe]=f[fe],Rl(c,m,null)}catch(h){throw f!==null&&Gc(f),Gc(m),h}finally{F(A.DynamicComponentEnd),Ri()}return new Xi(this.componentType,m,!!u)}};function T_(e,t,n,r){let o=e?["ng-version","21.2.5"]:ob(t.selectors[0]),i=null,s=null,a=0;if(n)for(let d of n)a+=d[nl].requiredVars,d.create&&(d.targetIdx=0,(i??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(r)for(let d=0;d<r.length;d++){let u=r[d];if(typeof u!="function")for(let m of u.bindings){a+=m[nl].requiredVars;let f=d+1;m.create&&(m.targetIdx=f,(i??=[]).push(m)),m.update&&(m.targetIdx=f,(s??=[]).push(m))}}let c=[t];if(r)for(let d of r){let u=typeof d=="function"?d:d.type,m=Ya(u);c.push(m)}return _l(0,null,S_(i,s),1,a,c,null,null,null,[o],null)}function S_(e,t){return!e&&!t?null:n=>{if(n&1&&e)for(let r of e)r.create();if(n&2&&t)for(let r of t)r.update()}}function qf(e){let t=e[nl].kind;return t==="input"||t==="twoWay"}var Xi=class extends ip{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(t,n,r){super(),this._rootLView=n,this._hasInputBindings=r,this._tNode=wi(n[D],K),this.location=er(this._tNode,n),this.instance=Ae(this._tNode.index,n)[fe],this.hostView=this.changeDetectorRef=new Vt(n,void 0),this.componentType=t}setInput(t,n){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),n))return;let o=this._rootLView,i=Nl(r,o[D],o,t,n);this.previousInputValues.set(t,n);let s=Ae(r.index,o);Ol(s,1)}get injector(){return new hn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function A_(e,t,n){let r=e.projection=[];for(let o=0;o<t.length;o++){let i=n[o];r.push(i!=null&&i.length?Array.from(i):null)}}var En=(()=>{class e{static __NG_ELEMENT_ID__=N_}return e})();function N_(){let e=be();return gp(e,T())}var ol=class e extends En{_lContainer;_hostTNode;_hostLView;constructor(t,n,r){super(),this._lContainer=t,this._hostTNode=n,this._hostLView=r}get element(){return er(this._hostTNode,this._hostLView)}get injector(){return new hn(this._hostTNode,this._hostLView)}get parentInjector(){let t=pl(this._hostTNode,this._hostLView);if(cm(t)){let n=zi(t,this._hostLView),r=$i(t),o=n[D].data[r+8];return new hn(o,n)}else return new hn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let n=Zf(this._lContainer);return n!==null&&n[t]||null}get length(){return this._lContainer.length-he}createEmbeddedView(t,n,r){let o,i;typeof r=="number"?o=r:r!=null&&(o=r.index,i=r.injector);let s=Jc(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(n||{},i,s);return this.insertImpl(a,o,Yi(this._hostTNode,s)),a}createComponent(t,n,r,o,i,s,a){let c=t&&!my(t),l;if(c)l=n;else{let y=n||{};l=y.index,r=y.injector,o=y.projectableNodes,i=y.environmentInjector||y.ngModuleRef,s=y.directives,a=y.bindings}let d=c?t:new Xn(Tt(t)),u=r||this.parentInjector;if(!i&&d.ngModule==null){let b=(c?u:this.parentInjector).get(de,null);b&&(i=b)}let m=Tt(d.componentType??{}),f=Jc(this._lContainer,m?.id??null),h=f?.firstChild??null,C=d.create(u,o,h,i,s,a);return this.insertImpl(C.hostView,l,Yi(this._hostTNode,f)),C}insert(t,n){return this.insertImpl(t,n,!0)}insertImpl(t,n,r){let o=t._lView;if(ef(o)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let c=o[ie],l=new e(c,c[ye],c[ie]);l.detach(l.indexOf(t))}}let i=this._adjustIndex(n),s=this._lContainer;return kl(s,o,i,r),t.attachToViewContainerRef(),ec(Pc(s),i,t),t}move(t,n){return this.insert(t,n)}indexOf(t){let n=Zf(this._lContainer);return n!==null?n.indexOf(t):-1}remove(t){let n=this._adjustIndex(t,-1),r=Qi(this._lContainer,n);r&&(Fr(Pc(this._lContainer),n),Ml(r[D],r))}detach(t){let n=this._adjustIndex(t,-1),r=Qi(this._lContainer,n);return r&&Fr(Pc(this._lContainer),n)!=null?new Vt(r):null}_adjustIndex(t,n=0){return t??this.length+n}};function Zf(e){return e[Pr]}function Pc(e){return e[Pr]||(e[Pr]=[])}function gp(e,t){let n,r=t[e.index];return Le(r)?n=r:(n=rp(r,t,null,e),t[e.index]=n,El(t,n)),F_(n,t,e,r),new ol(n,e,t)}function R_(e,t){let n=e[q],r=n.createComment(""),o=Ve(t,e),i=n.parentNode(o);return qi(n,i,r,n.nextSibling(o),!1),r}var F_=P_,O_=()=>!1;function k_(e,t,n){return O_(e,t,n)}function P_(e,t,n,r){if(e[Ft])return;let o;n.type&8?o=Se(r):o=R_(t,n),e[Ft]=o}var il=class e{queryList;matches=null;constructor(t){this.queryList=t}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},sl=class e{queries;constructor(t=[]){this.queries=t}createEmbeddedView(t){let n=t.queries;if(n!==null){let r=t.contentQueries!==null?t.contentQueries[0]:n.length,o=[];for(let i=0;i<r;i++){let s=n.getByIndex(i),a=this.queries[s.indexInDeclarationView];o.push(a.clone())}return new e(o)}return null}insertView(t){this.dirtyQueriesWithMatches(t)}detachView(t){this.dirtyQueriesWithMatches(t)}finishViewCreation(t){this.dirtyQueriesWithMatches(t)}dirtyQueriesWithMatches(t){for(let n=0;n<this.queries.length;n++)Vl(t,n).matches!==null&&this.queries[n].setDirty()}},Ji=class{flags;read;predicate;constructor(t,n,r=null){this.flags=n,this.read=r,typeof t=="string"?this.predicate=H_(t):this.predicate=t}},al=class e{queries;constructor(t=[]){this.queries=t}elementStart(t,n){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(t,n)}elementEnd(t){for(let n=0;n<this.queries.length;n++)this.queries[n].elementEnd(t)}embeddedTView(t){let n=null;for(let r=0;r<this.length;r++){let o=n!==null?n.length:0,i=this.getByIndex(r).embeddedTView(t,o);i&&(i.indexInDeclarationView=r,n!==null?n.push(i):n=[i])}return n!==null?new e(n):null}template(t,n){for(let r=0;r<this.queries.length;r++)this.queries[r].template(t,n)}getByIndex(t){return this.queries[t]}get length(){return this.queries.length}track(t){this.queries.push(t)}},cl=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(t,n=-1){this.metadata=t,this._declarationNodeIndex=n}elementStart(t,n){this.isApplyingToNode(n)&&this.matchTNode(t,n)}elementEnd(t){this._declarationNodeIndex===t.index&&(this._appliesToNextNode=!1)}template(t,n){this.elementStart(t,n)}embeddedTView(t,n){return this.isApplyingToNode(t)?(this.crossesNgTemplate=!0,this.addMatch(-t.index,n),new e(this.metadata)):null}isApplyingToNode(t){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let n=this._declarationNodeIndex,r=t.parent;for(;r!==null&&r.type&8&&r.index!==n;)r=r.parent;return n===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(t,n){let r=this.metadata.predicate;if(Array.isArray(r))for(let o=0;o<r.length;o++){let i=r[o];this.matchTNodeWithReadOption(t,n,L_(n,i)),this.matchTNodeWithReadOption(t,n,Bi(n,t,i,!1,!1))}else r===vn?n.type&4&&this.matchTNodeWithReadOption(t,n,-1):this.matchTNodeWithReadOption(t,n,Bi(n,t,r,!1,!1))}matchTNodeWithReadOption(t,n,r){if(r!==null){let o=this.metadata.read;if(o!==null)if(o===X||o===En||o===vn&&n.type&4)this.addMatch(n.index,-2);else{let i=Bi(n,t,o,!1,!1);i!==null&&this.addMatch(n.index,i)}else this.addMatch(n.index,r)}}addMatch(t,n){this.matches===null?this.matches=[t,n]:this.matches.push(t,n)}};function L_(e,t){let n=e.localNames;if(n!==null){for(let r=0;r<n.length;r+=2)if(n[r]===t)return n[r+1]}return null}function V_(e,t){return e.type&11?er(e,t):e.type&4?cs(e,t):null}function j_(e,t,n,r){return n===-1?V_(t,e):n===-2?B_(e,t,r):Gr(e,e[D],n,t)}function B_(e,t,n){if(n===X)return er(t,e);if(n===vn)return cs(t,e);if(n===En)return gp(t,e)}function vp(e,t,n,r){let o=t[et].queries[r];if(o.matches===null){let i=e.data,s=n.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=i[l];a.push(j_(t,d,s[c+1],n.metadata.read))}}o.matches=a}return o.matches}function ll(e,t,n,r){let o=e.queries.getByIndex(n),i=o.matches;if(i!==null){let s=vp(e,t,o,n);for(let a=0;a<i.length;a+=2){let c=i[a];if(c>0)r.push(s[a/2]);else{let l=i[a+1],d=t[-c];for(let u=he;u<d.length;u++){let m=d[u];m[Nt]===m[ie]&&ll(m[D],m,l,r)}if(d[un]!==null){let u=d[un];for(let m=0;m<u.length;m++){let f=u[m];ll(f[D],f,l,r)}}}}}return r}function Ll(e,t){return e[et].queries[t].queryList}function yp(e,t,n){let r=new Wi((n&4)===4);return rf(e,t,r,r.destroy),(t[et]??=new sl).queries.push(new il(r))-1}function bp(e,t,n){let r=ne();return r.firstCreatePass&&(Dp(r,new Ji(e,t,n),-1),(t&2)===2&&(r.staticViewQueries=!0)),yp(r,T(),t)}function _p(e,t,n,r){let o=ne();if(o.firstCreatePass){let i=be();Dp(o,new Ji(t,n,r),i.index),U_(o,e),(n&2)===2&&(o.staticContentQueries=!0)}return yp(o,T(),n)}function H_(e){return e.split(",").map(t=>t.trim())}function Dp(e,t,n){e.queries===null&&(e.queries=new al),e.queries.track(new cl(t,n))}function U_(e,t){let n=e.contentQueries||(e.contentQueries=[]),r=n.length?n[n.length-1]:-1;t!==r&&n.push(e.queries.length-1,t)}function Vl(e,t){return e.queries.getByIndex(t)}function Ep(e,t){let n=e[D],r=Vl(n,t);return r.crossesNgTemplate?ll(n,e,t,[]):vp(n,e,r,t)}function Cp(e,t,n){let r,o=_r(()=>{r._dirtyCounter();let i=$_(r,e);if(t&&i===void 0)throw new _(-951,!1);return i});return r=o[ae],r._dirtyCounter=Ne(0),r._flatValue=void 0,o}function jl(e){return Cp(!0,!1,e)}function Bl(e){return Cp(!0,!0,e)}function wp(e,t){let n=e[ae];n._lView=T(),n._queryIndex=t,n._queryList=Ll(n._lView,t),n._queryList.onDirty(()=>n._dirtyCounter.update(r=>r+1))}function $_(e,t){let n=e._lView,r=e._queryIndex;if(n===void 0||r===void 0||n[E]&4)return t?void 0:pe;let o=Ll(n,r),i=Ep(n,r);return o.reset(i,vm),t?o.first:o._changesDetected||e._flatValue===void 0?e._flatValue=o.toArray():e._flatValue}var qr=class{};var Zr=class extends qr{injector;componentFactoryResolver=new rl(this);instance=null;constructor(t){super();let n=new sn([...t.providers,{provide:qr,useValue:this},{provide:ds,useValue:this.componentFactoryResolver}],t.parent||Vn(),t.debugName,new Set(["environment"]));this.injector=n,t.runEnvironmentInitializers&&n.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function Ip(e,t,n=null){return new Zr({providers:e,parent:t,debugName:n,runEnvironmentInitializers:!0}).injector}var z_=(()=>{class e{_injector;cachedInjectors=new Map;constructor(n){this._injector=n}getOrCreateStandaloneInjector(n){if(!n.standalone)return null;if(!this.cachedInjectors.has(n)){let r=rc(!1,n.type),o=r.length>0?Ip([r],this._injector,""):null;this.cachedInjectors.set(n,o)}return this.cachedInjectors.get(n)}ngOnDestroy(){try{for(let n of this.cachedInjectors.values())n!==null&&n.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=v({token:e,providedIn:"environment",factory:()=>new e(M(de))})}return e})();function se(e){return Qr(()=>{let t=xp(e),n=N(I({},t),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===hl.OnPush,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&e.dependencies||null,getStandaloneInjector:t.standalone?o=>o.get(z_).getOrCreateStandaloneInjector(n):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||He.Emulated,styles:e.styles||pe,_:null,schemas:e.schemas||null,tView:null,id:""});t.standalone&&Kr("NgStandalone"),Mp(n);let r=e.dependencies;return n.directiveDefs=Yf(r,G_),n.pipeDefs=Yf(r,Lu),n.id=Z_(n),n})}function G_(e){return Tt(e)||Ya(e)}function $(e){return Qr(()=>({type:e.type,bootstrap:e.bootstrap||pe,declarations:e.declarations||pe,imports:e.imports||pe,exports:e.exports||pe,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function W_(e,t){if(e==null)return St;let n={};for(let r in e)if(e.hasOwnProperty(r)){let o=e[r],i,s,a,c;Array.isArray(o)?(a=o[0],i=o[1],s=o[2]??i,c=o[3]||null):(i=o,s=o,a=os.None,c=null),n[i]=[r,a,c],t[i]=s}return n}function q_(e){if(e==null)return St;let t={};for(let n in e)e.hasOwnProperty(n)&&(t[e[n]]=n);return t}function k(e){return Qr(()=>{let t=xp(e);return Mp(t),t})}function xp(e){let t={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:t,inputConfig:e.inputs||St,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||pe,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:W_(e.inputs,t),outputs:q_(e.outputs),debugInfo:null}}function Mp(e){e.features?.forEach(t=>t(e))}function Yf(e,t){return e?()=>{let n=typeof e=="function"?e():e,r=[];for(let o of n){let i=t(o);i!==null&&r.push(i)}return r}:null}function Z_(e){let t=0,n=typeof e.consts=="function"?"":e.consts,r=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,n,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let i of r.join("|"))t=Math.imul(31,t)+i.charCodeAt(0)<<0;return t+=2147483648,"c"+t}function Y_(e){return Object.getPrototypeOf(e.prototype).constructor}function Ie(e){let t=Y_(e.type),n=!0,r=[e];for(;t;){let o;if(tt(e))o=t.\u0275cmp||t.\u0275dir;else{if(t.\u0275cmp)throw new _(903,!1);o=t.\u0275dir}if(o){if(n){r.push(o);let s=e;s.inputs=Lc(e.inputs),s.declaredInputs=Lc(e.declaredInputs),s.outputs=Lc(e.outputs);let a=o.hostBindings;a&&eD(e,a);let c=o.viewQuery,l=o.contentQueries;if(c&&X_(e,c),l&&J_(e,l),Q_(e,o),Pu(e.outputs,o.outputs),tt(o)&&o.data.animation){let d=e.data;d.animation=(d.animation||[]).concat(o.data.animation)}}let i=o.features;if(i)for(let s=0;s<i.length;s++){let a=i[s];a&&a.ngInherit&&a(e),a===Ie&&(n=!1)}}t=Object.getPrototypeOf(t)}K_(r)}function Q_(e,t){for(let n in t.inputs){if(!t.inputs.hasOwnProperty(n)||e.inputs.hasOwnProperty(n))continue;let r=t.inputs[n];r!==void 0&&(e.inputs[n]=r,e.declaredInputs[n]=t.declaredInputs[n])}}function K_(e){let t=0,n=null;for(let r=e.length-1;r>=0;r--){let o=e[r];o.hostVars=t+=o.hostVars,o.hostAttrs=Yn(o.hostAttrs,n=Yn(n,o.hostAttrs))}}function Lc(e){return e===St?{}:e===pe?[]:e}function X_(e,t){let n=e.viewQuery;n?e.viewQuery=(r,o)=>{t(r,o),n(r,o)}:e.viewQuery=t}function J_(e,t){let n=e.contentQueries;n?e.contentQueries=(r,o,i)=>{t(r,o,i),n(r,o,i)}:e.contentQueries=t}function eD(e,t){let n=e.hostBindings;n?e.hostBindings=(r,o)=>{t(r,o),n(r,o)}:e.hostBindings=t}function Tp(e,t,n,r,o,i,s,a){if(n.firstCreatePass){e.mergedAttrs=Yn(e.mergedAttrs,e.attrs);let d=e.tView=_l(2,e,o,i,s,n.directiveRegistry,n.pipeRegistry,null,n.schemas,n.consts,null);n.queries!==null&&(n.queries.template(n,e),d.queries=n.queries.embeddedTView(e))}a&&(e.flags|=a),Gn(e,!1);let c=nD(n,t,e,r);Oi()&&Tl(n,t,c,e),Qn(c,t);let l=rp(c,t,c,e);t[r+K]=l,El(t,l),k_(l,e,t)}function tD(e,t,n,r,o,i,s,a,c,l,d){let u=n+K,m;return t.firstCreatePass?(m=rr(t,u,4,s||null,a||null),gc()&&ap(t,e,m,nt(t.consts,l),Gm),sm(t,m)):m=t.data[u],Tp(m,e,t,n,r,o,i,c),Lr(m)&&Al(t,e,m),l!=null&&as(e,m,d),m}function Hl(e,t,n,r,o,i,s,a,c,l,d){let u=n+K,m;if(t.firstCreatePass){if(m=rr(t,u,4,s||null,a||null),l!=null){let f=nt(t.consts,l);m.localNames=[];for(let h=0;h<f.length;h+=2)m.localNames.push(f[h],-1)}}else m=t.data[u];return Tp(m,e,t,n,r,o,i,c),l!=null&&as(e,m,d),m}function jt(e,t,n,r,o,i,s,a){let c=T(),l=ne(),d=nt(l.consts,i);return tD(c,l,e,t,n,r,o,d,void 0,s,a),jt}var nD=rD;function rD(e,t,n,r){return ki(!0),t[q].createComment("")}var Ul=new g("");function or(e){return!!e&&typeof e.then=="function"}function $l(e){return!!e&&typeof e.subscribe=="function"}var Sp=new g("");var zl=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((n,r)=>{this.resolve=n,this.reject=r});appInits=p(Sp,{optional:!0})??[];injector=p(ue);constructor(){}runInitializers(){if(this.initialized)return;let n=[];for(let o of this.appInits){let i=jn(this.injector,o);if(or(i))n.push(i);else if($l(i)){let s=new Promise((a,c)=>{i.subscribe({complete:a,error:c})});n.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(n).then(()=>{r()}).catch(o=>{this.reject(o)}),n.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Ap=new g("");function Np(){ia(()=>{let e="";throw new _(600,e)})}function Rp(e){return e.isBoundToModule}var oD=10;var Cn=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=p(Lt);afterRenderManager=p(wl);zonelessEnabled=p(Hr);rootEffectScheduler=p(Pi);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new G;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=p(pn);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Q(n=>!n))}constructor(){p(it,{optional:!0})}whenStable(){let n;return new Promise(r=>{n=this.isStable.subscribe({next:o=>{o&&r()}})}).finally(()=>{n.unsubscribe()})}_injector=p(de);_rendererFactory=null;get injector(){return this._injector}bootstrap(n,r){return this.bootstrapImpl(n,r)}bootstrapImpl(n,r,o=ue.NULL){return this._injector.get(R).run(()=>{F(A.BootstrapComponentStart);let s=n instanceof ls;if(!this._injector.get(zl).done){let h="";throw new _(405,h)}let c;s?c=n:c=this._injector.get(ds).resolveComponentFactory(n),this.componentTypes.push(c.componentType);let l=Rp(c)?void 0:this._injector.get(qr),d=r||c.selector,u=c.create(o,[],d,l),m=u.location.nativeElement,f=u.injector.get(Ul,null);return f?.registerApplication(m),u.onDestroy(()=>{this.detachView(u.hostView),zr(this.components,u),f?.unregisterApplication(m)}),this._loadComponent(u),F(A.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){F(A.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(is.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw F(A.ChangeDetectionEnd),new _(101,!1);let n=w(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,w(n),this.afterTick.next(),F(A.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ue,null,{optional:!0}));let n=0;for(;this.dirtyFlags!==0&&n++<oD;){F(A.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{F(A.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let n=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:o}of this.allViews){if(!r&&!Vr(o))continue;let i=r&&!this.zonelessEnabled?0:1;Jm(o,i),n=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}n||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:n})=>Vr(n))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(n){let r=n;this._views.push(r),r.attachToAppRef(this)}detachView(n){let r=n;zr(this._views,r),r.detachFromAppRef()}_loadComponent(n){this.attachView(n.hostView);try{this.tick()}catch(o){this.internalErrorHandler(o)}this.components.push(n),this._injector.get(Ap,[]).forEach(o=>o(n))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(n=>n()),this._views.slice().forEach(n=>n.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(n){return this._destroyListeners.push(n),()=>zr(this._destroyListeners,n)}destroy(){if(this._destroyed)throw new _(406,!1);let n=this._injector;n.destroy&&!n.destroyed&&n.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function zr(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function st(e,t,n,r){let o=T(),i=Wn();if(Dn(o,i,t)){let s=ne(),a=Fi();Ob(a,o,e,t,n,r)}return st}function _e(e,t,n,r,o,i,s,a){Kr("NgControlFlow");let c=T(),l=ne(),d=nt(l.consts,i);return Hl(c,l,e,t,n,r,o,d,256,s,a),Gl}function Gl(e,t,n,r,o,i,s,a){Kr("NgControlFlow");let c=T(),l=ne(),d=nt(l.consts,i);return Hl(c,l,e,t,n,r,o,d,512,s,a),Gl}function De(e,t){Kr("NgControlFlow");let n=T(),r=Wn(),o=n[r]!==$e?n[r]:-1,i=o!==-1?Qf(n,K+o):void 0,s=0;if(Dn(n,r,e)){let a=w(null);try{if(i!==void 0&&e_(i,s),e!==-1){let c=K+e,l=Qf(n,c),d=iD(n[D],c),u=c_(l,d,n),m=Fl(n,d,t,{dehydratedView:u});kl(l,m,s,Yi(d,u))}}finally{w(a)}}else if(i!==void 0){let a=Jb(i,s);a!==void 0&&(a[fe]=t)}}function Qf(e,t){return e[t]}function iD(e,t){return wi(e,t)}function ze(e,t,n){let r=T(),o=Wn();if(Dn(r,o,t)){let i=ne(),s=Fi();Sb(s,r,e,t,r[q],n)}return ze}function dl(e,t,n,r,o){Nl(t,e,n,o?"class":"style",r)}function B(e,t,n,r){let o=T(),i=o[D],s=e+K,a=i.firstCreatePass?lp(s,o,2,t,Gm,gc(),n,r):i.data[s];if(gt(a)){let c=o[Je].tracingService;if(c&&c.componentCreate){let l=i.data[a.directiveStart+a.componentOffset];return c.componentCreate(pp(l),()=>(Kf(e,t,o,a,r),B))}}return Kf(e,t,o,a,r),B}function Kf(e,t,n,r,o){if(Wm(r,n,e,t,Fp),Lr(r)){let i=n[D];Al(i,n,r),Mm(i,r,n)}o!=null&&as(n,r)}function H(){let e=ne(),t=be(),n=qm(t);return e.firstCreatePass&&dp(e,n),yc(n)&&bc(),hc(),n.classesWithoutHost!=null&&by(n)&&dl(e,n,T(),n.classesWithoutHost,!0),n.stylesWithoutHost!=null&&_y(n)&&dl(e,n,T(),n.stylesWithoutHost,!1),H}function Re(e,t,n,r){return B(e,t,n,r),H(),Re}function Bt(e,t,n,r){let o=T(),i=o[D],s=e+K,a=i.firstCreatePass?b_(s,i,2,t,n,r):i.data[s];return Wm(a,o,e,t,Fp),r!=null&&as(o,a),Bt}function Ht(){let e=be(),t=qm(e);return yc(t)&&bc(),hc(),Ht}function yt(e,t,n,r){return Bt(e,t,n,r),Ht(),yt}var Fp=(e,t,n,r,o)=>(ki(!0),Tm(t[q],r,yf()));function Wl(){return T()}function Xr(e,t,n){let r=T(),o=Wn();if(Dn(r,o,t)){let i=ne(),s=Fi();zm(s,r,e,t,r[q],n)}return Xr}var Jr="en-US";var sD=Jr;function Op(e){typeof e=="string"&&(sD=e.toLowerCase().replace(/_/g,"-"))}function Fe(e,t,n){let r=T(),o=ne(),i=be();return aD(o,r,r[q],i,e,t,n),Fe}function aD(e,t,n,r,o,i,s){let a=!0,c=null;if((r.type&3||s)&&(c??=kc(r,t,i),__(r,e,t,s,n,o,i,c)&&(a=!1)),a){let l=r.outputs?.[o],d=r.hostDirectiveOutputs?.[o];if(d&&d.length)for(let u=0;u<d.length;u+=2){let m=d[u],f=d[u+1];c??=kc(r,t,i),Wf(r,t,m,f,o,c)}if(l&&l.length)for(let u of l)c??=kc(r,t,i),Wf(r,t,u,o,o,c)}}function Ge(e=1){return vf(e)}function cD(e,t){let n=null,r=Xy(e);for(let o=0;o<t.length;o++){let i=t[o];if(i==="*"){n=o;continue}if(r===null?Rm(e,i,!0):tb(r,i))return o}return n}function bt(e){let t=T()[Ee][ye];if(!t.projection){let n=e?e.length:1,r=t.projection=$u(n,null),o=r.slice(),i=t.child;for(;i!==null;){if(i.type!==128){let s=e?cD(i,e):0;s!==null&&(o[s]?o[s].projectionNext=i:r[s]=i,o[s]=i)}i=i.next}}}function re(e,t=0,n,r,o,i){let s=T(),a=ne(),c=r?e+1:null;c!==null&&Hl(s,a,c,r,o,i,null,n);let l=rr(a,K+e,16,null,n||null);l.projection===null&&(l.projection=t),Ec();let u=!s[Bn]||vc();s[Ee][ye].projection[l.projection]===null&&c!==null?lD(s,a,c):u&&!rs(l)&&Eb(a,s,l)}function lD(e,t,n){let r=K+n,o=t.data[r],i=e[r],s=Jc(i,o.tView.ssrId),a=Fl(e,o,void 0,{dehydratedView:s});kl(i,a,0,Yi(o,s))}function us(e,t,n,r){return _p(e,t,n,r),us}function eo(e,t,n){return bp(e,t,n),eo}function ge(e){let t=T(),n=ne(),r=Ai();jr(r+1);let o=Vl(n,r);if(e.dirty&&Ju(t)===((o.metadata.flags&2)===2)){if(o.matches===null)e.reset([]);else{let i=Ep(t,r);e.reset(i,vm),e.notifyOnChanges()}return!0}return!1}function ve(){return Ll(T(),Ai())}function fs(e,t,n,r,o){return wp(t,_p(e,n,r,o)),fs}function ms(e,t,n,r){return wp(e,bp(t,n,r)),ms}function ps(e=1){jr(Ai()+e)}function hs(e){let t=cf();return Xu(t,K+e)}function Li(e,t){return e<<17|t<<2}function yn(e){return e>>17&32767}function dD(e){return(e&2)==2}function uD(e,t){return e&131071|t<<17}function ul(e){return e|2}function Jn(e){return(e&131068)>>2}function Vc(e,t){return e&-131069|t<<2}function fD(e){return(e&1)===1}function fl(e){return e|1}function mD(e,t,n,r,o,i){let s=i?t.classBindings:t.styleBindings,a=yn(s),c=Jn(s);e[r]=n;let l=!1,d;if(Array.isArray(n)){let u=n;d=u[1],(d===null||Ln(u,d)>0)&&(l=!0)}else d=n;if(o)if(c!==0){let m=yn(e[a+1]);e[r+1]=Li(m,a),m!==0&&(e[m+1]=Vc(e[m+1],r)),e[a+1]=uD(e[a+1],r)}else e[r+1]=Li(a,0),a!==0&&(e[a+1]=Vc(e[a+1],r)),a=r;else e[r+1]=Li(c,0),a===0?a=r:e[c+1]=Vc(e[c+1],r),c=r;l&&(e[r+1]=ul(e[r+1])),Xf(e,d,r,!0),Xf(e,d,r,!1),pD(t,d,e,r,i),s=Li(a,c),i?t.classBindings=s:t.styleBindings=s}function pD(e,t,n,r,o){let i=o?e.residualClasses:e.residualStyles;i!=null&&typeof t=="string"&&Ln(i,t)>=0&&(n[r+1]=fl(n[r+1]))}function Xf(e,t,n,r){let o=e[n+1],i=t===null,s=r?yn(o):Jn(o),a=!1;for(;s!==0&&(a===!1||i);){let c=e[s],l=e[s+1];hD(c,t)&&(a=!0,e[s+1]=r?fl(l):ul(l)),s=r?yn(l):Jn(l)}a&&(e[n+1]=r?ul(o):fl(o))}function hD(e,t){return e===null||t==null||(Array.isArray(e)?e[1]:e)===t?!0:Array.isArray(e)&&typeof t=="string"?Ln(e,t)>=0:!1}var Be={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function gD(e){return e.substring(Be.key,Be.keyEnd)}function vD(e){return yD(e),kp(e,Pp(e,0,Be.textEnd))}function kp(e,t){let n=Be.textEnd;return n===t?-1:(t=Be.keyEnd=bD(e,Be.key=t,n),Pp(e,t,n))}function yD(e){Be.key=0,Be.keyEnd=0,Be.value=0,Be.valueEnd=0,Be.textEnd=e.length}function Pp(e,t,n){for(;t<n&&e.charCodeAt(t)<=32;)t++;return t}function bD(e,t,n){for(;t<n&&e.charCodeAt(t)>32;)t++;return t}function J(e,t){return DD(e,t,null,!0),J}function ql(e){ED(TD,_D,e,!0)}function _D(e,t){for(let n=vD(t);n>=0;n=kp(t,n))bi(e,gD(t),!0)}function DD(e,t,n,r){let o=T(),i=ne(),s=wc(2);if(i.firstUpdatePass&&Vp(i,e,s,r),t!==$e&&Dn(o,s,t)){let a=i.data[Ot()];jp(i,a,o,o[q],e,o[s+1]=AD(t,n),r,s)}}function ED(e,t,n,r){let o=ne(),i=wc(2);o.firstUpdatePass&&Vp(o,null,i,r);let s=T();if(n!==$e&&Dn(s,i,n)){let a=o.data[Ot()];if(Bp(a,r)&&!Lp(o,i)){let c=r?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(n=pi(c,n||"")),dl(o,a,s,n,r)}else SD(o,a,s,s[q],s[i+1],s[i+1]=MD(e,t,n),r,i)}}function Lp(e,t){return t>=e.expandoStartIndex}function Vp(e,t,n,r){let o=e.data;if(o[n+1]===null){let i=o[Ot()],s=Lp(e,n);Bp(i,r)&&t===null&&!s&&(t=!1),t=CD(o,i,t,r),mD(o,i,t,n,s,r)}}function CD(e,t,n,r){let o=mf(e),i=r?t.residualClasses:t.residualStyles;if(o===null)(r?t.classBindings:t.styleBindings)===0&&(n=jc(null,e,t,n,r),n=Yr(n,t.attrs,r),i=null);else{let s=t.directiveStylingLast;if(s===-1||e[s]!==o)if(n=jc(o,e,t,n,r),i===null){let c=wD(e,t,r);c!==void 0&&Array.isArray(c)&&(c=jc(null,e,t,c[1],r),c=Yr(c,t.attrs,r),ID(e,t,r,c))}else i=xD(e,t,r)}return i!==void 0&&(r?t.residualClasses=i:t.residualStyles=i),n}function wD(e,t,n){let r=n?t.classBindings:t.styleBindings;if(Jn(r)!==0)return e[yn(r)]}function ID(e,t,n,r){let o=n?t.classBindings:t.styleBindings;e[yn(o)]=r}function xD(e,t,n){let r,o=t.directiveEnd;for(let i=1+t.directiveStylingLast;i<o;i++){let s=e[i].hostAttrs;r=Yr(r,s,n)}return Yr(r,t.attrs,n)}function jc(e,t,n,r,o){let i=null,s=n.directiveEnd,a=n.directiveStylingLast;for(a===-1?a=n.directiveStart:a++;a<s&&(i=t[a],r=Yr(r,i.hostAttrs,o),i!==e);)a++;return e!==null&&(n.directiveStylingLast=a),r}function Yr(e,t,n){let r=n?1:2,o=-1;if(t!==null)for(let i=0;i<t.length;i++){let s=t[i];typeof s=="number"?o=s:o===r&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),bi(e,s,n?!0:t[++i]))}return e===void 0?null:e}function MD(e,t,n){if(n==null||n==="")return pe;let r=[],o=yl(n);if(Array.isArray(o))for(let i=0;i<o.length;i++)e(r,o[i],!0);else if(o instanceof Set)for(let i of o)e(r,i,!0);else if(typeof o=="object")for(let i in o)o.hasOwnProperty(i)&&e(r,i,o[i]);else typeof o=="string"&&t(r,o);return r}function TD(e,t,n){let r=String(t);r!==""&&!r.includes(" ")&&bi(e,r,n)}function SD(e,t,n,r,o,i,s,a){o===$e&&(o=pe);let c=0,l=0,d=0<o.length?o[0]:null,u=0<i.length?i[0]:null;for(;d!==null||u!==null;){let m=c<o.length?o[c+1]:void 0,f=l<i.length?i[l+1]:void 0,h=null,C;d===u?(c+=2,l+=2,m!==f&&(h=u,C=f)):u===null||d!==null&&d<u?(c+=2,h=d):(l+=2,h=u,C=f),h!==null&&jp(e,t,n,r,h,C,s,a),d=c<o.length?o[c]:null,u=l<i.length?i[l]:null}}function jp(e,t,n,r,o,i,s,a){if(!(t.type&3))return;let c=e.data,l=c[a+1],d=fD(l)?Jf(c,t,n,o,Jn(l),s):void 0;if(!es(d)){es(i)||dD(l)&&(i=Jf(c,null,n,o,a,s));let u=lc(Ot(),n);wb(r,s,u,o,i)}}function Jf(e,t,n,r,o,i){let s=t===null,a;for(;o>0;){let c=e[o],l=Array.isArray(c),d=l?c[1]:c,u=d===null,m=n[o+1];m===$e&&(m=u?pe:void 0);let f=u?_i(m,r):d===r?m:void 0;if(l&&!es(f)&&(f=_i(c,r)),es(f)&&(a=f,s))return a;let h=e[o+1];o=s?yn(h):Jn(h)}if(t!==null){let c=i?t.residualClasses:t.residualStyles;c!=null&&(a=_i(c,r))}return a}function es(e){return e!==void 0}function AD(e,t){return e==null||e===""||(typeof t=="string"?e=e+t:typeof e=="object"&&(e=mi(yl(e)))),e}function Bp(e,t){return(e.flags&(t?8:16))!==0}function xe(e,t=""){let n=T(),r=ne(),o=e+K,i=r.firstCreatePass?rr(r,o,1,t,null):r.data[o],s=ND(r,n,i,t);n[o]=s,Oi()&&Tl(r,n,s,i),Gn(i,!1)}var ND=(e,t,n,r)=>(ki(!0),Uy(t[q],r));function RD(e,t,n,r=""){return Dn(e,Wn(),n)?t+Ka(n)+r:$e}function ir(e){return sr("",e),ir}function sr(e,t,n){let r=T(),o=RD(r,e,t,n);return o!==$e&&FD(r,Ot(),o),sr}function FD(e,t,n){let r=lc(t,e);$y(e[q],r,n)}function em(e,t,n){let r=ne();r.firstCreatePass&&Hp(t,r.data,r.blueprint,tt(e),n)}function Hp(e,t,n,r,o){if(e=ce(e),Array.isArray(e))for(let i=0;i<e.length;i++)Hp(e[i],t,n,r,o);else{let i=ne(),s=T(),a=be(),c=on(e)?e:ce(e.provide),l=ic(e),d=a.providerIndexes&1048575,u=a.directiveStart,m=a.providerIndexes>>20;if(on(e)||!e.multi){let f=new gn(l,o,j,null),h=Hc(c,t,o?d:d+m,u);h===-1?($c(Gi(a,s),i,c),Bc(i,e,t.length),t.push(c),a.directiveStart++,a.directiveEnd++,o&&(a.providerIndexes+=1048576),n.push(f),s.push(f)):(n[h]=f,s[h]=f)}else{let f=Hc(c,t,d+m,u),h=Hc(c,t,d,d+m),C=f>=0&&n[f],y=h>=0&&n[h];if(o&&!y||!o&&!C){$c(Gi(a,s),i,c);let b=PD(o?kD:OD,n.length,o,r,l,e);!o&&y&&(n[h].providerFactory=b),Bc(i,e,t.length,0),t.push(c),a.directiveStart++,a.directiveEnd++,o&&(a.providerIndexes+=1048576),n.push(b),s.push(b)}else{let b=Up(n[o?h:f],l,!o&&r);Bc(i,e,f>-1?f:h,b)}!o&&r&&y&&n[h].componentProviders++}}}function Bc(e,t,n,r){let o=on(t),i=qu(t);if(o||i){let c=(i?ce(t.useClass):t).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!o&&t.multi){let d=l.indexOf(n);d===-1?l.push(n,[r,c]):l[d+1].push(r,c)}else l.push(n,c)}}}function Up(e,t,n){return n&&e.componentProviders++,e.multi.push(t)-1}function Hc(e,t,n,r){for(let o=n;o<r;o++)if(t[o]===e)return o;return-1}function OD(e,t,n,r,o){return ml(this.multi,[])}function kD(e,t,n,r,o){let i=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Gr(r,r[D],this.providerFactory.index,o);s=c.slice(0,a),ml(i,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],ml(i,s);return s}function ml(e,t){for(let n=0;n<e.length;n++){let r=e[n];t.push(r())}return t}function PD(e,t,n,r,o,i){let s=new gn(e,n,j,null);return s.multi=[],s.index=t,s.componentProviders=0,Up(s,o,r&&!n),s}function _t(e,t){return n=>{n.providersResolver=(r,o)=>em(r,o?o(e):e,!1),t&&(n.viewProvidersResolver=(r,o)=>em(r,o?o(t):t,!0))}}function Zl(e,t){return cs(e,t)}var $p=(()=>{class e{applicationErrorHandler=p(Lt);appRef=p(Cn);taskService=p(pn);ngZone=p(R);zonelessEnabled=p(Hr);tracing=p(it,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new te;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ar):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(p(Ac,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let n=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(n);return}this.switchToMicrotaskScheduler(),this.taskService.remove(n)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let n=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(n)})})}notify(n){if(!this.zonelessEnabled&&n===5)return;switch(n){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?Cf:Mc;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ar+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let n=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(n),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let n=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(n)}}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function zp(){return[{provide:Xe,useExisting:$p},{provide:R,useClass:Nr},{provide:Hr,useValue:!0}]}function LD(){return typeof $localize<"u"&&$localize.locale||Jr}var gs=new g("",{factory:()=>p(gs,{optional:!0,skipSelf:!0})||LD()});function at(e){return Nu(e)}function ct(e,t){return _r(e,t?.equal)}function Gp(e,t){return jl(t)}function JD(e,t){return Bl(t)}var no=(Gp.required=JD,Gp);function Wp(e,t){return jl(t)}function eE(e,t){return Bl(t)}var Qp=(Wp.required=eE,Wp);var Ql=new g(""),tE=new g("");function to(e){return!e.moduleRef}function nE(e){let t=to(e)?e.r3Injector:e.moduleRef.injector,n=t.get(R);return n.run(()=>{to(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let r=t.get(Lt),o;if(n.runOutsideAngular(()=>{o=n.onError.subscribe({next:r})}),to(e)){let i=()=>t.destroy(),s=e.platformInjector.get(Ql);s.add(i),t.onDestroy(()=>{o.unsubscribe(),s.delete(i)})}else{let i=()=>e.moduleRef.destroy(),s=e.platformInjector.get(Ql);s.add(i),e.moduleRef.onDestroy(()=>{zr(e.allPlatformModules,e.moduleRef),o.unsubscribe(),s.delete(i)})}return oE(r,n,()=>{let i=t.get(pn),s=i.add(),a=t.get(zl);return a.runInitializers(),a.donePromise.then(()=>{let c=t.get(gs,Jr);if(Op(c||Jr),!t.get(tE,!0))return to(e)?t.get(Cn):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(to(e)){let d=t.get(Cn);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return rE?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{i.remove(s)})})})}var rE;function oE(e,t,n){try{let r=n();return or(r)?r.catch(o=>{throw t.runOutsideAngular(()=>e(o)),o}):r}catch(r){throw t.runOutsideAngular(()=>e(r)),r}}var vs=null;function iE(e=[],t){return ue.create({name:t,providers:[{provide:kr,useValue:"platform"},{provide:Ql,useValue:new Set([()=>vs=null])},...e]})}function sE(e=[]){if(vs)return vs;let t=iE(e);return vs=t,Np(),aE(t),t}function aE(e){let t=e.get(ns,null);jn(e,()=>{t?.forEach(n=>n())})}var cE=1e4;var C1=cE-1e3;var ar=(()=>{class e{static __NG_ELEMENT_ID__=lE}return e})();function lE(e){return dE(be(),T(),(e&16)===16)}function dE(e,t,n){if(gt(e)&&!n){let r=Ae(e.index,t);return new Vt(r,r)}else if(e.type&175){let r=t[Ee];return new Vt(r,t)}return null}var Kl=class{supports(t){return Pl(t)}create(t){return new Xl(t)}},uE=(e,t)=>t,Xl=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(t){this._trackByFn=t||uE}forEachItem(t){let n;for(n=this._itHead;n!==null;n=n._next)t(n)}forEachOperation(t){let n=this._itHead,r=this._removalsHead,o=0,i=null;for(;n||r;){let s=!r||n&&n.currentIndex<qp(r,o,i)?n:r,a=qp(s,o,i),c=s.currentIndex;if(s===r)o--,r=r._nextRemoved;else if(n=n._next,s.previousIndex==null)o++;else{i||(i=[]);let l=a-o,d=c-o;if(l!=d){for(let m=0;m<l;m++){let f=m<i.length?i[m]:i[m]=0,h=f+m;d<=h&&h<l&&(i[m]=f+1)}let u=s.previousIndex;i[u]=d-l}}a!==c&&t(s,a,c)}}forEachPreviousItem(t){let n;for(n=this._previousItHead;n!==null;n=n._nextPrevious)t(n)}forEachAddedItem(t){let n;for(n=this._additionsHead;n!==null;n=n._nextAdded)t(n)}forEachMovedItem(t){let n;for(n=this._movesHead;n!==null;n=n._nextMoved)t(n)}forEachRemovedItem(t){let n;for(n=this._removalsHead;n!==null;n=n._nextRemoved)t(n)}forEachIdentityChange(t){let n;for(n=this._identityChangesHead;n!==null;n=n._nextIdentityChange)t(n)}diff(t){if(t==null&&(t=[]),!Pl(t))throw new _(900,!1);return this.check(t)?this:null}onDestroy(){}check(t){this._reset();let n=this._itHead,r=!1,o,i,s;if(Array.isArray(t)){this.length=t.length;for(let a=0;a<this.length;a++)i=t[a],s=this._trackByFn(a,i),n===null||!Object.is(n.trackById,s)?(n=this._mismatch(n,i,s,a),r=!0):(r&&(n=this._verifyReinsertion(n,i,s,a)),Object.is(n.item,i)||this._addIdentityChange(n,i)),n=n._next}else o=0,up(t,a=>{s=this._trackByFn(o,a),n===null||!Object.is(n.trackById,s)?(n=this._mismatch(n,a,s,o),r=!0):(r&&(n=this._verifyReinsertion(n,a,s,o)),Object.is(n.item,a)||this._addIdentityChange(n,a)),n=n._next,o++}),this.length=o;return this._truncate(n),this.collection=t,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let t;for(t=this._previousItHead=this._itHead;t!==null;t=t._next)t._nextPrevious=t._next;for(t=this._additionsHead;t!==null;t=t._nextAdded)t.previousIndex=t.currentIndex;for(this._additionsHead=this._additionsTail=null,t=this._movesHead;t!==null;t=t._nextMoved)t.previousIndex=t.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(t,n,r,o){let i;return t===null?i=this._itTail:(i=t._prev,this._remove(t)),t=this._unlinkedRecords===null?null:this._unlinkedRecords.get(r,null),t!==null?(Object.is(t.item,n)||this._addIdentityChange(t,n),this._reinsertAfter(t,i,o)):(t=this._linkedRecords===null?null:this._linkedRecords.get(r,o),t!==null?(Object.is(t.item,n)||this._addIdentityChange(t,n),this._moveAfter(t,i,o)):t=this._addAfter(new Jl(n,r),i,o)),t}_verifyReinsertion(t,n,r,o){let i=this._unlinkedRecords===null?null:this._unlinkedRecords.get(r,null);return i!==null?t=this._reinsertAfter(i,t._prev,o):t.currentIndex!=o&&(t.currentIndex=o,this._addToMoves(t,o)),t}_truncate(t){for(;t!==null;){let n=t._next;this._addToRemovals(this._unlink(t)),t=n}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(t,n,r){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(t);let o=t._prevRemoved,i=t._nextRemoved;return o===null?this._removalsHead=i:o._nextRemoved=i,i===null?this._removalsTail=o:i._prevRemoved=o,this._insertAfter(t,n,r),this._addToMoves(t,r),t}_moveAfter(t,n,r){return this._unlink(t),this._insertAfter(t,n,r),this._addToMoves(t,r),t}_addAfter(t,n,r){return this._insertAfter(t,n,r),this._additionsTail===null?this._additionsTail=this._additionsHead=t:this._additionsTail=this._additionsTail._nextAdded=t,t}_insertAfter(t,n,r){let o=n===null?this._itHead:n._next;return t._next=o,t._prev=n,o===null?this._itTail=t:o._prev=t,n===null?this._itHead=t:n._next=t,this._linkedRecords===null&&(this._linkedRecords=new ys),this._linkedRecords.put(t),t.currentIndex=r,t}_remove(t){return this._addToRemovals(this._unlink(t))}_unlink(t){this._linkedRecords!==null&&this._linkedRecords.remove(t);let n=t._prev,r=t._next;return n===null?this._itHead=r:n._next=r,r===null?this._itTail=n:r._prev=n,t}_addToMoves(t,n){return t.previousIndex===n||(this._movesTail===null?this._movesTail=this._movesHead=t:this._movesTail=this._movesTail._nextMoved=t),t}_addToRemovals(t){return this._unlinkedRecords===null&&(this._unlinkedRecords=new ys),this._unlinkedRecords.put(t),t.currentIndex=null,t._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=t,t._prevRemoved=null):(t._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=t),t}_addIdentityChange(t,n){return t.item=n,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=t:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=t,t}},Jl=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(t,n){this.item=t,this.trackById=n}},ed=class{_head=null;_tail=null;add(t){this._head===null?(this._head=this._tail=t,t._nextDup=null,t._prevDup=null):(this._tail._nextDup=t,t._prevDup=this._tail,t._nextDup=null,this._tail=t)}get(t,n){let r;for(r=this._head;r!==null;r=r._nextDup)if((n===null||n<=r.currentIndex)&&Object.is(r.trackById,t))return r;return null}remove(t){let n=t._prevDup,r=t._nextDup;return n===null?this._head=r:n._nextDup=r,r===null?this._tail=n:r._prevDup=n,this._head===null}},ys=class{map=new Map;put(t){let n=t.trackById,r=this.map.get(n);r||(r=new ed,this.map.set(n,r)),r.add(t)}get(t,n){let r=t,o=this.map.get(r);return o?o.get(t,n):null}remove(t){let n=t.trackById;return this.map.get(n).remove(t)&&this.map.delete(n),t}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function qp(e,t,n){let r=e.previousIndex;if(r===null)return r;let o=0;return n&&r<n.length&&(o=n[r]),r+t+o}function Zp(){return new nd([new Kl])}var nd=(()=>{class e{factories;static \u0275prov=v({token:e,providedIn:"root",factory:Zp});constructor(n){this.factories=n}static create(n,r){if(r!=null){let o=r.factories.slice();n=n.concat(o)}return new e(n)}static extend(n){return{provide:e,useFactory:()=>{let r=p(e,{optional:!0,skipSelf:!0});return e.create(n,r||Zp())}}}find(n){let r=this.factories.find(o=>o.supports(n));if(r!=null)return r;throw new _(901,!1)}}return e})();function Kp(e){let{rootComponent:t,appProviders:n,platformProviders:r,platformRef:o}=e;F(A.BootstrapApplicationStart);try{let i=o?.injector??sE(r),s=[zp(),If,...n||[]],a=new Zr({providers:s,parent:i,debugName:"",runEnvironmentInitializers:!1});return nE({r3Injector:a.injector,platformInjector:i,rootComponent:t})}catch(i){return Promise.reject(i)}finally{F(A.BootstrapApplicationEnd)}}function cr(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Xp(e,t=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):t}var Yl=Symbol("NOT_SET"),Jp=new Set,fE=N(I({},Fo),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Yl,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Yl&&!Tn(this))return this.signal;try{for(let o of this.cleanup??Jp)o()}finally{this.cleanup?.clear()}let t=[];e!==void 0&&t.push(e),t.push(this.registerCleanupFn);let n=Dt(this),r;try{r=this.userFn.apply(null,t)}finally{qt(this,n)}return(this.value===Yl||!this.equal(this.value,r))&&(this.value=r,this.version++),this.signal}}),td=class extends Zi{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(t,n,r,o,i,s=null){super(t,[void 0,void 0,void 0,void 0],r,!1,i.get(Pt),s),this.scheduler=o;for(let a of Il){let c=n[a];if(c===void 0)continue;let l=Object.create(fE);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Mn(l),l.value),l.signal[ae]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();super.destroy();for(let t of this.nodes)if(t)try{for(let n of t.cleanup??Jp)n()}finally{Et(t)}}};function eh(e,t){let n=t?.injector??p(ue),r=n.get(Xe),o=n.get(wl),i=n.get(it,null,{optional:!0});o.impl??=n.get(Pm);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=n.get(Br,null,{optional:!0}),c=new td(o.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,r,n,i?.snapshot(null));return o.impl.register(c),c}function th(e,t){let n=Tt(e),r=t.elementInjector||Vn();return new Xn(n).create(r,t.projectableNodes,t.hostElement,t.environmentInjector,t.directives,t.bindings)}var nh=null;function We(){return nh}function rd(e){nh??=e}var ro=class{},bs=(()=>{class e{historyGo(n){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:()=>p(rh),providedIn:"platform"})}return e})();var rh=(()=>{class e extends bs{_location;_history;_doc=p(Z);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return We().getBaseHref(this._doc)}onPopState(n){let r=We().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",n,!1),()=>r.removeEventListener("popstate",n)}onHashChange(n){let r=We().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",n,!1),()=>r.removeEventListener("hashchange",n)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(n){this._location.pathname=n}pushState(n,r,o){this._history.pushState(n,r,o)}replaceState(n,r,o){this._history.replaceState(n,r,o)}forward(){this._history.forward()}back(){this._history.back()}historyGo(n=0){this._history.go(n)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:()=>new e,providedIn:"platform"})}return e})();var _s=class{$implicit;ngForOf;index;count;constructor(t,n,r,o){this.$implicit=t,this.ngForOf=n,this.index=r,this.count=o}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Ds=(()=>{class e{_viewContainer;_template;_differs;set ngForOf(n){this._ngForOf=n,this._ngForOfDirty=!0}set ngForTrackBy(n){this._trackByFn=n}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(n,r,o){this._viewContainer=n,this._template=r,this._differs=o}set ngForTemplate(n){n&&(this._template=n)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let n=this._ngForOf;!this._differ&&n&&(this._differ=this._differs.find(n).create(this.ngForTrackBy))}if(this._differ){let n=this._differ.diff(this._ngForOf);n&&this._applyChanges(n)}}_applyChanges(n){let r=this._viewContainer;n.forEachOperation((o,i,s)=>{if(o.previousIndex==null)r.createEmbeddedView(this._template,new _s(o.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)r.remove(i===null?void 0:i);else if(i!==null){let a=r.get(i);r.move(a,s),oh(a,o)}});for(let o=0,i=r.length;o<i;o++){let a=r.get(o).context;a.index=o,a.count=i,a.ngForOf=this._ngForOf}n.forEachIdentityChange(o=>{let i=r.get(o.currentIndex);oh(i,o)})}static ngTemplateContextGuard(n,r){return!0}static \u0275fac=function(r){return new(r||e)(j(En),j(vn),j(nd))};static \u0275dir=k({type:e,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return e})();function oh(e,t){e.context.$implicit=t.item}var od=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=p(ue);constructor(n){this._viewContainerRef=n}ngOnChanges(n){if(this._shouldRecreateView(n)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let o=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,o,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(n){return!!n.ngTemplateOutlet||!!n.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(n,r,o)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,o):!1,get:(n,r,o)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,o)}})}static \u0275fac=function(r){return new(r||e)(j(En))};static \u0275dir=k({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[bn]})}return e})();var Es=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();function oo(e,t){t=encodeURIComponent(t);for(let n of e.split(";")){let r=n.indexOf("="),[o,i]=r==-1?[n,""]:[n.slice(0,r),n.slice(r+1)];if(o.trim()===t)return decodeURIComponent(i)}return null}var wn=class{};var id="browser";function ih(e){return e===id}var io=class{_doc;constructor(t){this._doc=t}manager},Cs=(()=>{class e extends io{constructor(n){super(n)}supports(n){return!0}addEventListener(n,r,o,i){return n.addEventListener(r,o,i),()=>this.removeEventListener(n,r,o,i)}removeEventListener(n,r,o,i){return n.removeEventListener(r,o,i)}static \u0275fac=function(r){return new(r||e)(M(Z))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),xs=new g(""),ld=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(n,r){this._zone=r,n.forEach(s=>{s.manager=this});let o=n.filter(s=>!(s instanceof Cs));this._plugins=o.slice().reverse();let i=n.find(s=>s instanceof Cs);i&&this._plugins.push(i)}addEventListener(n,r,o,i){return this._findPluginFor(r).addEventListener(n,r,o,i)}getZone(){return this._zone}_findPluginFor(n){let r=this._eventNameToPlugin.get(n);if(r)return r;if(r=this._plugins.find(i=>i.supports(n)),!r)throw new _(5101,!1);return this._eventNameToPlugin.set(n,r),r}static \u0275fac=function(r){return new(r||e)(M(xs),M(R))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),sd="ng-app-id";function sh(e){for(let t of e)t.remove()}function ah(e,t){let n=t.createElement("style");return n.textContent=e,n}function pE(e,t,n,r){let o=e.head?.querySelectorAll(`style[${sd}="${t}"],link[${sd}="${t}"]`);if(o)for(let i of o)i.removeAttribute(sd),i instanceof HTMLLinkElement?r.set(i.href.slice(i.href.lastIndexOf("/")+1),{usage:0,elements:[i]}):i.textContent&&n.set(i.textContent,{usage:0,elements:[i]})}function cd(e,t){let n=t.createElement("link");return n.setAttribute("rel","stylesheet"),n.setAttribute("href",e),n}var dd=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(n,r,o,i={}){this.doc=n,this.appId=r,this.nonce=o,pE(n,r,this.inline,this.external),this.hosts.add(n.head)}addStyles(n,r){for(let o of n)this.addUsage(o,this.inline,ah);r?.forEach(o=>this.addUsage(o,this.external,cd))}removeStyles(n,r){for(let o of n)this.removeUsage(o,this.inline);r?.forEach(o=>this.removeUsage(o,this.external))}addUsage(n,r,o){let i=r.get(n);i?i.usage++:r.set(n,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,o(n,this.doc)))})}removeUsage(n,r){let o=r.get(n);o&&(o.usage--,o.usage<=0&&(sh(o.elements),r.delete(n)))}ngOnDestroy(){for(let[,{elements:n}]of[...this.inline,...this.external])sh(n);this.hosts.clear()}addHost(n){this.hosts.add(n);for(let[r,{elements:o}]of this.inline)o.push(this.addElement(n,ah(r,this.doc)));for(let[r,{elements:o}]of this.external)o.push(this.addElement(n,cd(r,this.doc)))}removeHost(n){this.hosts.delete(n)}addElement(n,r){return this.nonce&&r.setAttribute("nonce",this.nonce),n.appendChild(r)}static \u0275fac=function(r){return new(r||e)(M(Z),M(tr),M(nr,8),M(_n))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),ad={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},ud=/%COMP%/g;var lh="%COMP%",hE=`_nghost-${lh}`,gE=`_ngcontent-${lh}`,vE=!0,yE=new g("",{factory:()=>vE});function bE(e){return gE.replace(ud,e)}function _E(e){return hE.replace(ud,e)}function dh(e,t){return t.map(n=>n.replace(ud,e))}var fd=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(n,r,o,i,s,a,c=null,l=null){this.eventManager=n,this.sharedStylesHost=r,this.appId=o,this.removeStylesOnCompDestroy=i,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new so(n,s,a,this.tracingService)}createRenderer(n,r){if(!n||!r)return this.defaultRenderer;let o=this.getOrCreateRenderer(n,r);return o instanceof Is?o.applyToHost(n):o instanceof ao&&o.applyStyles(),o}getOrCreateRenderer(n,r){let o=this.rendererByCompId,i=o.get(r.id);if(!i){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(r.encapsulation){case He.Emulated:i=new Is(c,l,r,this.appId,d,s,a,u);break;case He.ShadowDom:return new ws(c,n,r,s,a,this.nonce,u,l);case He.ExperimentalIsolatedShadowDom:return new ws(c,n,r,s,a,this.nonce,u);default:i=new ao(c,l,r,d,s,a,u);break}o.set(r.id,i)}return i}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(n){this.rendererByCompId.delete(n)}static \u0275fac=function(r){return new(r||e)(M(ld),M(dd),M(tr),M(yE),M(Z),M(R),M(nr),M(it,8))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),so=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(t,n,r,o){this.eventManager=t,this.doc=n,this.ngZone=r,this.tracingService=o}destroy(){}destroyNode=null;createElement(t,n){return n?this.doc.createElementNS(ad[n]||n,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,n){(ch(t)?t.content:t).appendChild(n)}insertBefore(t,n,r){t&&(ch(t)?t.content:t).insertBefore(n,r)}removeChild(t,n){n.remove()}selectRootElement(t,n){let r=typeof t=="string"?this.doc.querySelector(t):t;if(!r)throw new _(-5104,!1);return n||(r.textContent=""),r}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,n,r,o){if(o){n=o+":"+n;let i=ad[o];i?t.setAttributeNS(i,n,r):t.setAttribute(n,r)}else t.setAttribute(n,r)}removeAttribute(t,n,r){if(r){let o=ad[r];o?t.removeAttributeNS(o,n):t.removeAttribute(`${r}:${n}`)}else t.removeAttribute(n)}addClass(t,n){t.classList.add(n)}removeClass(t,n){t.classList.remove(n)}setStyle(t,n,r,o){o&(ot.DashCase|ot.Important)?t.style.setProperty(n,r,o&ot.Important?"important":""):t.style[n]=r}removeStyle(t,n,r){r&ot.DashCase?t.style.removeProperty(n):t.style[n]=""}setProperty(t,n,r){t!=null&&(t[n]=r)}setValue(t,n){t.nodeValue=n}listen(t,n,r,o){if(typeof t=="string"&&(t=We().getGlobalEventTarget(this.doc,t),!t))throw new _(5102,!1);let i=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(i=this.tracingService.wrapEventListener(t,n,i)),this.eventManager.addEventListener(t,n,i,o)}decoratePreventDefault(t){return n=>{if(n==="__ngUnwrap__")return t;t(n)===!1&&n.preventDefault()}}};function ch(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var ws=class extends so{hostEl;sharedStylesHost;shadowRoot;constructor(t,n,r,o,i,s,a,c){super(t,o,i,a),this.hostEl=n,this.sharedStylesHost=c,this.shadowRoot=n.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=r.styles;l=dh(r.id,l);for(let u of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=u,this.shadowRoot.appendChild(m)}let d=r.getExternalStyles?.();if(d)for(let u of d){let m=cd(u,o);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,n){return super.appendChild(this.nodeOrShadowRoot(t),n)}insertBefore(t,n,r){return super.insertBefore(this.nodeOrShadowRoot(t),n,r)}removeChild(t,n){return super.removeChild(null,n)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ao=class extends so{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(t,n,r,o,i,s,a,c){super(t,i,s,a),this.sharedStylesHost=n,this.removeStylesOnCompDestroy=o;let l=r.styles;this.styles=c?dh(c,l):l,this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Kn.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Is=class extends ao{contentAttr;hostAttr;constructor(t,n,r,o,i,s,a,c){let l=o+"-"+r.id;super(t,n,r,i,s,a,c,l),this.contentAttr=bE(l),this.hostAttr=_E(l)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,n){let r=super.createElement(t,n);return super.setAttribute(r,this.contentAttr,""),r}};var Ms=class e extends ro{supportsDOMEvents=!0;static makeCurrent(){rd(new e)}onAndCancel(t,n,r,o){return t.addEventListener(n,r,o),()=>{t.removeEventListener(n,r,o)}}dispatchEvent(t,n){t.dispatchEvent(n)}remove(t){t.remove()}createElement(t,n){return n=n||this.getDefaultDocument(),n.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,n){return n==="window"?window:n==="document"?t:n==="body"?t.body:null}getBaseHref(t){let n=DE();return n==null?null:EE(n)}resetBaseElement(){co=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return oo(document.cookie,t)}},co=null;function DE(){return co=co||document.head.querySelector("base"),co?co.getAttribute("href"):null}function EE(e){return new URL(e,document.baseURI).pathname}var CE=(()=>{class e{build(){return new XMLHttpRequest}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})(),uh=["alt","control","meta","shift"],wE={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},IE={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},fh=(()=>{class e extends io{constructor(n){super(n)}supports(n){return e.parseEventName(n)!=null}addEventListener(n,r,o,i){let s=e.parseEventName(r),a=e.eventCallback(s.fullKey,o,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>We().onAndCancel(n,s.domEventName,a,i))}static parseEventName(n){let r=n.toLowerCase().split("."),o=r.shift();if(r.length===0||!(o==="keydown"||o==="keyup"))return null;let i=e._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),uh.forEach(l=>{let d=r.indexOf(l);d>-1&&(r.splice(d,1),s+=l+".")}),s+=i,r.length!=0||i.length===0)return null;let c={};return c.domEventName=o,c.fullKey=s,c}static matchEventFullKeyCode(n,r){let o=wE[n.key]||n.key,i="";return r.indexOf("code.")>-1&&(o=n.code,i="code."),o==null||!o?!1:(o=o.toLowerCase(),o===" "?o="space":o==="."&&(o="dot"),uh.forEach(s=>{if(s!==o){let a=IE[s];a(n)&&(i+=s+".")}}),i+=o,i===r)}static eventCallback(n,r,o){return i=>{e.matchEventFullKeyCode(i,n)&&o.runGuarded(()=>r(i))}}static _normalizeKey(n){return n==="esc"?"escape":n}static \u0275fac=function(r){return new(r||e)(M(Z))};static \u0275prov=v({token:e,factory:e.\u0275fac})}return e})();async function md(e,t,n){let r=I({rootComponent:e},xE(t,n));return Kp(r)}function xE(e,t){return{platformRef:t?.platformRef,appProviders:[...NE,...e?.providers??[]],platformProviders:AE}}function ME(){Ms.makeCurrent()}function TE(){return new Ke}function SE(){return gl(document),document}var AE=[{provide:_n,useValue:id},{provide:ns,useValue:ME,multi:!0},{provide:Z,useFactory:SE}];var NE=[{provide:kr,useValue:"root"},{provide:Ke,useFactory:TE},{provide:xs,useClass:Cs,multi:!0},{provide:xs,useClass:fh,multi:!0},fd,dd,ld,{provide:Ue,useExisting:fd},{provide:wn,useClass:CE},[]];var In=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(t){t?typeof t=="string"?this.lazyInit=()=>{this.headers=new Map,t.split(`
`).forEach(n=>{let r=n.indexOf(":");if(r>0){let o=n.slice(0,r),i=n.slice(r+1).trim();this.addHeaderEntry(o,i)}})}:typeof Headers<"u"&&t instanceof Headers?(this.headers=new Map,t.forEach((n,r)=>{this.addHeaderEntry(r,n)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(t).forEach(([n,r])=>{this.setHeaderEntries(n,r)})}:this.headers=new Map}has(t){return this.init(),this.headers.has(t.toLowerCase())}get(t){this.init();let n=this.headers.get(t.toLowerCase());return n&&n.length>0?n[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(t){return this.init(),this.headers.get(t.toLowerCase())||null}append(t,n){return this.clone({name:t,value:n,op:"a"})}set(t,n){return this.clone({name:t,value:n,op:"s"})}delete(t,n){return this.clone({name:t,value:n,op:"d"})}maybeSetNormalizedName(t,n){this.normalizedNames.has(n)||this.normalizedNames.set(n,t)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(t=>this.applyUpdate(t)),this.lazyUpdate=null))}copyFrom(t){t.init(),Array.from(t.headers.keys()).forEach(n=>{this.headers.set(n,t.headers.get(n)),this.normalizedNames.set(n,t.normalizedNames.get(n))})}clone(t){let n=new e;return n.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,n.lazyUpdate=(this.lazyUpdate||[]).concat([t]),n}applyUpdate(t){let n=t.name.toLowerCase();switch(t.op){case"a":case"s":let r=t.value;if(typeof r=="string"&&(r=[r]),r.length===0)return;this.maybeSetNormalizedName(t.name,n);let o=(t.op==="a"?this.headers.get(n):void 0)||[];o.push(...r),this.headers.set(n,o);break;case"d":let i=t.value;if(!i)this.headers.delete(n),this.normalizedNames.delete(n);else{let s=this.headers.get(n);if(!s)return;s=s.filter(a=>i.indexOf(a)===-1),s.length===0?(this.headers.delete(n),this.normalizedNames.delete(n)):this.headers.set(n,s)}break}}addHeaderEntry(t,n){let r=t.toLowerCase();this.maybeSetNormalizedName(t,r),this.headers.has(r)?this.headers.get(r).push(n):this.headers.set(r,[n])}setHeaderEntries(t,n){let r=(Array.isArray(n)?n:[n]).map(i=>i.toString()),o=t.toLowerCase();this.headers.set(o,r),this.maybeSetNormalizedName(t,o)}forEach(t){this.init(),Array.from(this.normalizedNames.keys()).forEach(n=>t(this.normalizedNames.get(n),this.headers.get(n)))}};var Ss=class{map=new Map;set(t,n){return this.map.set(t,n),this}get(t){return this.map.has(t)||this.map.set(t,t.defaultValue()),this.map.get(t)}delete(t){return this.map.delete(t),this}has(t){return this.map.has(t)}keys(){return this.map.keys()}},As=class{encodeKey(t){return mh(t)}encodeValue(t){return mh(t)}decodeKey(t){return decodeURIComponent(t)}decodeValue(t){return decodeURIComponent(t)}};function RE(e,t){let n=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(o=>{let i=o.indexOf("="),[s,a]=i==-1?[t.decodeKey(o),""]:[t.decodeKey(o.slice(0,i)),t.decodeValue(o.slice(i+1))],c=n.get(s)||[];c.push(a),n.set(s,c)}),n}var FE=/%(\d[a-f0-9])/gi,OE={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function mh(e){return encodeURIComponent(e).replace(FE,(t,n)=>OE[n]??t)}function Ts(e){return`${e}`}var Ut=class e{map;encoder;updates=null;cloneFrom=null;constructor(t={}){if(this.encoder=t.encoder||new As,t.fromString){if(t.fromObject)throw new _(2805,!1);this.map=RE(t.fromString,this.encoder)}else t.fromObject?(this.map=new Map,Object.keys(t.fromObject).forEach(n=>{let r=t.fromObject[n],o=Array.isArray(r)?r.map(Ts):[Ts(r)];this.map.set(n,o)})):this.map=null}has(t){return this.init(),this.map.has(t)}get(t){this.init();let n=this.map.get(t);return n?n[0]:null}getAll(t){return this.init(),this.map.get(t)||null}keys(){return this.init(),Array.from(this.map.keys())}append(t,n){return this.clone({param:t,value:n,op:"a"})}appendAll(t){let n=[];return Object.keys(t).forEach(r=>{let o=t[r];Array.isArray(o)?o.forEach(i=>{n.push({param:r,value:i,op:"a"})}):n.push({param:r,value:o,op:"a"})}),this.clone(n)}set(t,n){return this.clone({param:t,value:n,op:"s"})}delete(t,n){return this.clone({param:t,value:n,op:"d"})}toString(){return this.init(),this.keys().map(t=>{let n=this.encoder.encodeKey(t);return this.map.get(t).map(r=>n+"="+this.encoder.encodeValue(r)).join("&")}).filter(t=>t!=="").join("&")}clone(t){let n=new e({encoder:this.encoder});return n.cloneFrom=this.cloneFrom||this,n.updates=(this.updates||[]).concat(t),n}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(t=>this.map.set(t,this.cloneFrom.map.get(t))),this.updates.forEach(t=>{switch(t.op){case"a":case"s":let n=(t.op==="a"?this.map.get(t.param):void 0)||[];n.push(Ts(t.value)),this.map.set(t.param,n);break;case"d":if(t.value!==void 0){let r=this.map.get(t.param)||[],o=r.indexOf(Ts(t.value));o!==-1&&r.splice(o,1),r.length>0?this.map.set(t.param,r):this.map.delete(t.param)}else{this.map.delete(t.param);break}}}),this.cloneFrom=this.updates=null)}};function kE(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function ph(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function hh(e){return typeof Blob<"u"&&e instanceof Blob}function gh(e){return typeof FormData<"u"&&e instanceof FormData}function PE(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var vh="Content-Type",yh="Accept",_h="text/plain",Dh="application/json",LE=`${Dh}, ${_h}, */*`,lo=class e{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(t,n,r,o){this.url=n,this.method=t.toUpperCase();let i;if(kE(this.method)||o?(this.body=r!==void 0?r:null,i=o):i=r,i){if(this.reportProgress=!!i.reportProgress,this.withCredentials=!!i.withCredentials,this.keepalive=!!i.keepalive,i.responseType&&(this.responseType=i.responseType),i.headers&&(this.headers=i.headers),i.context&&(this.context=i.context),i.params&&(this.params=i.params),i.priority&&(this.priority=i.priority),i.cache&&(this.cache=i.cache),i.credentials&&(this.credentials=i.credentials),typeof i.timeout=="number"){if(i.timeout<1||!Number.isInteger(i.timeout))throw new _(2822,"");this.timeout=i.timeout}i.mode&&(this.mode=i.mode),i.redirect&&(this.redirect=i.redirect),i.integrity&&(this.integrity=i.integrity),i.referrer&&(this.referrer=i.referrer),i.referrerPolicy&&(this.referrerPolicy=i.referrerPolicy),this.transferCache=i.transferCache}if(this.headers??=new In,this.context??=new Ss,!this.params)this.params=new Ut,this.urlWithParams=n;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=n;else{let a=n.indexOf("?"),c=a===-1?"?":a<n.length-1?"&":"";this.urlWithParams=n+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||ph(this.body)||hh(this.body)||gh(this.body)||PE(this.body)?this.body:this.body instanceof Ut?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||gh(this.body)?null:hh(this.body)?this.body.type||null:ph(this.body)?null:typeof this.body=="string"?_h:this.body instanceof Ut?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?Dh:null}clone(t={}){let n=t.method||this.method,r=t.url||this.url,o=t.responseType||this.responseType,i=t.keepalive??this.keepalive,s=t.priority||this.priority,a=t.cache||this.cache,c=t.mode||this.mode,l=t.redirect||this.redirect,d=t.credentials||this.credentials,u=t.referrer||this.referrer,m=t.integrity||this.integrity,f=t.referrerPolicy||this.referrerPolicy,h=t.transferCache??this.transferCache,C=t.timeout??this.timeout,y=t.body!==void 0?t.body:this.body,b=t.withCredentials??this.withCredentials,ee=t.reportProgress??this.reportProgress,dt=t.headers||this.headers,oe=t.params||this.params,vr=t.context??this.context;return t.setHeaders!==void 0&&(dt=Object.keys(t.setHeaders).reduce((yr,Gt)=>yr.set(Gt,t.setHeaders[Gt]),dt)),t.setParams&&(oe=Object.keys(t.setParams).reduce((yr,Gt)=>yr.set(Gt,t.setParams[Gt]),oe)),new e(n,r,y,{params:oe,headers:dt,context:vr,reportProgress:ee,responseType:o,withCredentials:b,transferCache:h,keepalive:i,cache:a,priority:s,timeout:C,mode:c,redirect:l,credentials:d,referrer:u,integrity:m,referrerPolicy:f})}},lr=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(lr||{}),dr=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(t,n=200,r="OK"){this.headers=t.headers||new In,this.status=t.status!==void 0?t.status:n,this.statusText=t.statusText||r,this.url=t.url||null,this.redirected=t.redirected,this.responseType=t.responseType,this.ok=this.status>=200&&this.status<300}},Ns=class e extends dr{constructor(t={}){super(t)}type=lr.ResponseHeader;clone(t={}){return new e({headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0})}},Rs=class e extends dr{body;constructor(t={}){super(t),this.body=t.body!==void 0?t.body:null}type=lr.Response;clone(t={}){return new e({body:t.body!==void 0?t.body:this.body,headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0,redirected:t.redirected??this.redirected,responseType:t.responseType??this.responseType})}},uo=class extends dr{name="HttpErrorResponse";message;error;ok=!1;constructor(t){super(t,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${t.url||"(unknown url)"}`:this.message=`Http failure response for ${t.url||"(unknown url)"}: ${t.status} ${t.statusText}`,this.error=t.error||null}},VE=200,jE=204;var BE=new g("");var HE=/^\)\]\}',?\n/;var hd=(()=>{class e{xhrFactory;tracingService=p(it,{optional:!0});constructor(n){this.xhrFactory=n}maybePropagateTrace(n){return this.tracingService?.propagate?this.tracingService.propagate(n):n}handle(n){if(n.method==="JSONP")throw new _(-2800,!1);let r=this.xhrFactory;return Ye(null).pipe(Sa(()=>new O(i=>{let s=r.build();if(s.open(n.method,n.urlWithParams),n.withCredentials&&(s.withCredentials=!0),n.headers.forEach((y,b)=>s.setRequestHeader(y,b.join(","))),n.headers.has(yh)||s.setRequestHeader(yh,LE),!n.headers.has(vh)){let y=n.detectContentTypeHeader();y!==null&&s.setRequestHeader(vh,y)}if(n.timeout&&(s.timeout=n.timeout),n.responseType){let y=n.responseType.toLowerCase();s.responseType=y!=="json"?y:"text"}let a=n.serializeBody(),c=null,l=()=>{if(c!==null)return c;let y=s.statusText||"OK",b=new In(s.getAllResponseHeaders()),ee=s.responseURL||n.url;return c=new Ns({headers:b,status:s.status,statusText:y,url:ee}),c},d=this.maybePropagateTrace(()=>{let{headers:y,status:b,statusText:ee,url:dt}=l(),oe=null;b!==jE&&(oe=typeof s.response>"u"?s.responseText:s.response),b===0&&(b=oe?VE:0);let vr=b>=200&&b<300;if(n.responseType==="json"&&typeof oe=="string"){let yr=oe;oe=oe.replace(HE,"");try{oe=oe!==""?JSON.parse(oe):null}catch(Gt){oe=yr,vr&&(vr=!1,oe={error:Gt,text:oe})}}vr?(i.next(new Rs({body:oe,headers:y,status:b,statusText:ee,url:dt||void 0})),i.complete()):i.error(new uo({error:oe,headers:y,status:b,statusText:ee,url:dt||void 0}))}),u=this.maybePropagateTrace(y=>{let{url:b}=l(),ee=new uo({error:y,status:s.status||0,statusText:s.statusText||"Unknown Error",url:b||void 0});i.error(ee)}),m=u;n.timeout&&(m=this.maybePropagateTrace(y=>{let{url:b}=l(),ee=new uo({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:b||void 0});i.error(ee)}));let f=!1,h=this.maybePropagateTrace(y=>{f||(i.next(l()),f=!0);let b={type:lr.DownloadProgress,loaded:y.loaded};y.lengthComputable&&(b.total=y.total),n.responseType==="text"&&s.responseText&&(b.partialText=s.responseText),i.next(b)}),C=this.maybePropagateTrace(y=>{let b={type:lr.UploadProgress,loaded:y.loaded};y.lengthComputable&&(b.total=y.total),i.next(b)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",m),s.addEventListener("abort",u),n.reportProgress&&(s.addEventListener("progress",h),a!==null&&s.upload&&s.upload.addEventListener("progress",C)),s.send(a),i.next({type:lr.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",m),n.reportProgress&&(s.removeEventListener("progress",h),a!==null&&s.upload&&s.upload.removeEventListener("progress",C)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(r){return new(r||e)(M(wn))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Eh(e,t){return t(e)}function UE(e,t){return(n,r)=>t.intercept(n,{handle:o=>e(o,r)})}function $E(e,t,n){return(r,o)=>jn(n,()=>t(r,i=>e(i,o)))}var Ch=new g(""),gd=new g("",{factory:()=>[]}),zE=new g(""),vd=new g("",{factory:()=>!0});function GE(){let e=null;return(t,n)=>{e===null&&(e=(p(Ch,{optional:!0})??[]).reduceRight(UE,Eh));let r=p(Ur);if(p(vd)){let i=r.add();return e(t,n).pipe(ii(i))}else return e(t,n)}}var yd=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:function(r){let o=null;return r?o=new(r||e):o=M(hd),o},providedIn:"root"})}return e})();var Fs=(()=>{class e{backend;injector;chain=null;pendingTasks=p(Ur);contributeToStability=p(vd);constructor(n,r){this.backend=n,this.injector=r}handle(n){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(gd),...this.injector.get(zE,[])]));this.chain=r.reduceRight((o,i)=>$E(o,i,this.injector),Eh)}if(this.contributeToStability){let r=this.pendingTasks.add();return this.chain(n,o=>this.backend.handle(o)).pipe(ii(r))}else return this.chain(n,r=>this.backend.handle(r))}static \u0275fac=function(r){return new(r||e)(M(yd),M(de))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),bd=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:function(r){let o=null;return r?o=new(r||e):o=M(Fs),o},providedIn:"root"})}return e})();function pd(e,t){return{body:t,headers:e.headers,context:e.context,observe:e.observe,params:e.params,reportProgress:e.reportProgress,responseType:e.responseType,withCredentials:e.withCredentials,credentials:e.credentials,transferCache:e.transferCache,timeout:e.timeout,keepalive:e.keepalive,priority:e.priority,cache:e.cache,mode:e.mode,redirect:e.redirect,integrity:e.integrity,referrer:e.referrer,referrerPolicy:e.referrerPolicy}}var WE=(()=>{class e{handler;constructor(n){this.handler=n}request(n,r,o={}){let i;if(n instanceof lo)i=n;else{let c;o.headers instanceof In?c=o.headers:c=new In(o.headers);let l;o.params&&(o.params instanceof Ut?l=o.params:l=new Ut({fromObject:o.params})),i=new lo(n,r,o.body!==void 0?o.body:null,{headers:c,context:o.context,params:l,reportProgress:o.reportProgress,responseType:o.responseType||"json",withCredentials:o.withCredentials,transferCache:o.transferCache,keepalive:o.keepalive,priority:o.priority,cache:o.cache,mode:o.mode,redirect:o.redirect,credentials:o.credentials,referrer:o.referrer,referrerPolicy:o.referrerPolicy,integrity:o.integrity,timeout:o.timeout})}let s=Ye(i).pipe(_a(c=>this.handler.handle(c)));if(n instanceof lo||o.observe==="events")return s;let a=s.pipe(mt(c=>c instanceof Rs));switch(o.observe||"body"){case"body":switch(i.responseType){case"arraybuffer":return a.pipe(Q(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new _(2806,!1);return c.body}));case"blob":return a.pipe(Q(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new _(2807,!1);return c.body}));case"text":return a.pipe(Q(c=>{if(c.body!==null&&typeof c.body!="string")throw new _(2808,!1);return c.body}));default:return a.pipe(Q(c=>c.body))}case"response":return a;default:throw new _(2809,!1)}}delete(n,r={}){return this.request("DELETE",n,r)}get(n,r={}){return this.request("GET",n,r)}head(n,r={}){return this.request("HEAD",n,r)}jsonp(n,r){return this.request("JSONP",n,{params:new Ut().append(r,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(n,r={}){return this.request("OPTIONS",n,r)}patch(n,r,o={}){return this.request("PATCH",n,pd(o,r))}post(n,r,o={}){return this.request("POST",n,pd(o,r))}put(n,r,o={}){return this.request("PUT",n,pd(o,r))}static \u0275fac=function(r){return new(r||e)(M(bd))};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var qE=new g("",{factory:()=>!0}),ZE="XSRF-TOKEN",YE=new g("",{factory:()=>ZE}),QE="X-XSRF-TOKEN",KE=new g("",{factory:()=>QE}),XE=(()=>{class e{cookieName=p(YE);doc=p(Z);lastCookieString="";lastToken=null;parseCount=0;getToken(){let n=this.doc.cookie||"";return n!==this.lastCookieString&&(this.parseCount++,this.lastToken=oo(n,this.cookieName),this.lastCookieString=n),this.lastToken}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),wh=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:function(r){let o=null;return r?o=new(r||e):o=M(XE),o},providedIn:"root"})}return e})();function JE(e,t){if(!p(qE)||e.method==="GET"||e.method==="HEAD")return t(e);try{let o=p(bs).href,{origin:i}=new URL(o),{origin:s}=new URL(e.url,i);if(i!==s)return t(e)}catch{return t(e)}let n=p(wh).getToken(),r=p(KE);return n!=null&&!e.headers.has(r)&&(e=e.clone({headers:e.headers.set(r,n)})),t(e)}var _d=(function(e){return e[e.Interceptors=0]="Interceptors",e[e.LegacyInterceptors=1]="LegacyInterceptors",e[e.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",e[e.NoXsrfProtection=3]="NoXsrfProtection",e[e.JsonpSupport=4]="JsonpSupport",e[e.RequestsMadeViaParent=5]="RequestsMadeViaParent",e[e.Fetch=6]="Fetch",e})(_d||{});function eC(e,t){return{\u0275kind:e,\u0275providers:t}}function Ih(...e){let t=[WE,Fs,{provide:bd,useExisting:Fs},{provide:yd,useFactory:()=>p(BE,{optional:!0})??p(hd)},{provide:gd,useValue:JE,multi:!0}];for(let n of e)t.push(...n.\u0275providers);return Di(t)}var bh=new g("");function xh(){return eC(_d.LegacyInterceptors,[{provide:bh,useFactory:GE},{provide:gd,useExisting:bh,multi:!0}])}var Dd=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({providers:[Ih(xh())]})}return e})();var Oh=(()=>{class e{_renderer;_elementRef;onChange=n=>{};onTouched=()=>{};constructor(n,r){this._renderer=n,this._elementRef=r}setProperty(n,r){this._renderer.setProperty(this._elementRef.nativeElement,n,r)}registerOnTouched(n){this.onTouched=n}registerOnChange(n){this.onChange=n}setDisabledState(n){this.setProperty("disabled",n)}static \u0275fac=function(r){return new(r||e)(j(vt),j(X))};static \u0275dir=k({type:e})}return e})(),tC=(()=>{class e extends Oh{static \u0275fac=(()=>{let n;return function(o){return(n||(n=ts(e)))(o||e)}})();static \u0275dir=k({type:e,features:[Ie]})}return e})(),kh=new g("");var nC={provide:kh,useExisting:Mt(()=>zs),multi:!0};function rC(){let e=We()?We().getUserAgent():"";return/android (\d+)/.test(e.toLowerCase())}var oC=new g(""),zs=(()=>{class e extends Oh{_compositionMode;_composing=!1;constructor(n,r,o){super(n,r),this._compositionMode=o,this._compositionMode==null&&(this._compositionMode=!rC())}writeValue(n){let r=n??"";this.setProperty("value",r)}_handleInput(n){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(n)}_compositionStart(){this._composing=!0}_compositionEnd(n){this._composing=!1,this._compositionMode&&this.onChange(n)}static \u0275fac=function(r){return new(r||e)(j(vt),j(X),j(oC,8))};static \u0275dir=k({type:e,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(r,o){r&1&&Fe("input",function(s){return o._handleInput(s.target.value)})("blur",function(){return o.onTouched()})("compositionstart",function(){return o._compositionStart()})("compositionend",function(s){return o._compositionEnd(s.target.value)})},standalone:!1,features:[_t([nC]),Ie]})}return e})();var Ph=new g(""),Lh=new g("");function Vh(e){return e!=null}function jh(e){return or(e)?ft(e):e}function Bh(e){let t={};return e.forEach(n=>{t=n!=null?I(I({},t),n):t}),Object.keys(t).length===0?null:t}function Hh(e,t){return t.map(n=>n(e))}function iC(e){return!e.validate}function Uh(e){return e.map(t=>iC(t)?t:n=>t.validate(n))}function sC(e){if(!e)return null;let t=e.filter(Vh);return t.length==0?null:function(n){return Bh(Hh(n,t))}}function $h(e){return e!=null?sC(Uh(e)):null}function aC(e){if(!e)return null;let t=e.filter(Vh);return t.length==0?null:function(n){let r=Hh(n,t).map(jh);return ya(r).pipe(Q(Bh))}}function zh(e){return e!=null?aC(Uh(e)):null}function Mh(e,t){return e===null?[t]:Array.isArray(e)?[...e,t]:[e,t]}function Gh(e){return e._rawValidators}function Wh(e){return e._rawAsyncValidators}function Ed(e){return e?Array.isArray(e)?e:[e]:[]}function ks(e,t){return Array.isArray(e)?e.includes(t):e===t}function Th(e,t){let n=Ed(t);return Ed(e).forEach(o=>{ks(n,o)||n.push(o)}),n}function Sh(e,t){return Ed(t).filter(n=>!ks(e,n))}var Ps=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(t){this._rawValidators=t||[],this._composedValidatorFn=$h(this._rawValidators)}_setAsyncValidators(t){this._rawAsyncValidators=t||[],this._composedAsyncValidatorFn=zh(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(t){this._onDestroyCallbacks.push(t)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(t=>t()),this._onDestroyCallbacks=[]}reset(t=void 0){this.control?.reset(t)}hasError(t,n){return this.control?this.control.hasError(t,n):!1}getError(t,n){return this.control?this.control.getError(t,n):null}},mr=class extends Ps{name;get formDirective(){return null}get path(){return null}},go=class extends Ps{_parent=null;name=null;valueAccessor=null},Ls=class{_cd;constructor(t){this._cd=t}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var qh=(()=>{class e extends Ls{constructor(n){super(n)}static \u0275fac=function(r){return new(r||e)(j(go,2))};static \u0275dir=k({type:e,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(r,o){r&2&&J("ng-untouched",o.isUntouched)("ng-touched",o.isTouched)("ng-pristine",o.isPristine)("ng-dirty",o.isDirty)("ng-valid",o.isValid)("ng-invalid",o.isInvalid)("ng-pending",o.isPending)},standalone:!1,features:[Ie]})}return e})(),Zh=(()=>{class e extends Ls{constructor(n){super(n)}static \u0275fac=function(r){return new(r||e)(j(mr,10))};static \u0275dir=k({type:e,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(r,o){r&2&&J("ng-untouched",o.isUntouched)("ng-touched",o.isTouched)("ng-pristine",o.isPristine)("ng-dirty",o.isDirty)("ng-valid",o.isValid)("ng-invalid",o.isInvalid)("ng-pending",o.isPending)("ng-submitted",o.isSubmitted)},standalone:!1,features:[Ie]})}return e})();var fo="VALID",Os="INVALID",ur="PENDING",mo="DISABLED",$t=class{},Vs=class extends $t{value;source;constructor(t,n){super(),this.value=t,this.source=n}},po=class extends $t{pristine;source;constructor(t,n){super(),this.pristine=t,this.source=n}},ho=class extends $t{touched;source;constructor(t,n){super(),this.touched=t,this.source=n}},fr=class extends $t{status;source;constructor(t,n){super(),this.status=t,this.source=n}},Cd=class extends $t{source;constructor(t){super(),this.source=t}},js=class extends $t{source;constructor(t){super(),this.source=t}};function Yh(e){return(Gs(e)?e.validators:e)||null}function cC(e){return Array.isArray(e)?$h(e):e||null}function Qh(e,t){return(Gs(t)?t.asyncValidators:e)||null}function lC(e){return Array.isArray(e)?zh(e):e||null}function Gs(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}function dC(e,t,n){let r=e.controls;if(!(t?Object.keys(r):r).length)throw new _(1e3,"");if(!r[n])throw new _(1001,"")}function uC(e,t,n){e._forEachChild((r,o)=>{if(n[o]===void 0)throw new _(1002,"")})}var Bs=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(t,n){this._assignValidators(t),this._assignAsyncValidators(n)}get validator(){return this._composedValidatorFn}set validator(t){this._rawValidators=this._composedValidatorFn=t}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(t){this._rawAsyncValidators=this._composedAsyncValidatorFn=t}get parent(){return this._parent}get status(){return at(this.statusReactive)}set status(t){at(()=>this.statusReactive.set(t))}_status=ct(()=>this.statusReactive());statusReactive=Ne(void 0);get valid(){return this.status===fo}get invalid(){return this.status===Os}get pending(){return this.status==ur}get disabled(){return this.status===mo}get enabled(){return this.status!==mo}errors;get pristine(){return at(this.pristineReactive)}set pristine(t){at(()=>this.pristineReactive.set(t))}_pristine=ct(()=>this.pristineReactive());pristineReactive=Ne(!0);get dirty(){return!this.pristine}get touched(){return at(this.touchedReactive)}set touched(t){at(()=>this.touchedReactive.set(t))}_touched=ct(()=>this.touchedReactive());touchedReactive=Ne(!1);get untouched(){return!this.touched}_events=new G;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(t){this._assignValidators(t)}setAsyncValidators(t){this._assignAsyncValidators(t)}addValidators(t){this.setValidators(Th(t,this._rawValidators))}addAsyncValidators(t){this.setAsyncValidators(Th(t,this._rawAsyncValidators))}removeValidators(t){this.setValidators(Sh(t,this._rawValidators))}removeAsyncValidators(t){this.setAsyncValidators(Sh(t,this._rawAsyncValidators))}hasValidator(t){return ks(this._rawValidators,t)}hasAsyncValidator(t){return ks(this._rawAsyncValidators,t)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(t={}){let n=this.touched===!1;this.touched=!0;let r=t.sourceControl??this;t.onlySelf||this._parent?.markAsTouched(N(I({},t),{sourceControl:r})),n&&t.emitEvent!==!1&&this._events.next(new ho(!0,r))}markAllAsDirty(t={}){this.markAsDirty({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(n=>n.markAllAsDirty(t))}markAllAsTouched(t={}){this.markAsTouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(n=>n.markAllAsTouched(t))}markAsUntouched(t={}){let n=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let r=t.sourceControl??this;this._forEachChild(o=>{o.markAsUntouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:r})}),t.onlySelf||this._parent?._updateTouched(t,r),n&&t.emitEvent!==!1&&this._events.next(new ho(!1,r))}markAsDirty(t={}){let n=this.pristine===!0;this.pristine=!1;let r=t.sourceControl??this;t.onlySelf||this._parent?.markAsDirty(N(I({},t),{sourceControl:r})),n&&t.emitEvent!==!1&&this._events.next(new po(!1,r))}markAsPristine(t={}){let n=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let r=t.sourceControl??this;this._forEachChild(o=>{o.markAsPristine({onlySelf:!0,emitEvent:t.emitEvent})}),t.onlySelf||this._parent?._updatePristine(t,r),n&&t.emitEvent!==!1&&this._events.next(new po(!0,r))}markAsPending(t={}){this.status=ur;let n=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new fr(this.status,n)),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.markAsPending(N(I({},t),{sourceControl:n}))}disable(t={}){let n=this._parentMarkedDirty(t.onlySelf);this.status=mo,this.errors=null,this._forEachChild(o=>{o.disable(N(I({},t),{onlySelf:!0}))}),this._updateValue();let r=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Vs(this.value,r)),this._events.next(new fr(this.status,r)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(N(I({},t),{skipPristineCheck:n}),this),this._onDisabledChange.forEach(o=>o(!0))}enable(t={}){let n=this._parentMarkedDirty(t.onlySelf);this.status=fo,this._forEachChild(r=>{r.enable(N(I({},t),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent}),this._updateAncestors(N(I({},t),{skipPristineCheck:n}),this),this._onDisabledChange.forEach(r=>r(!1))}_updateAncestors(t,n){t.onlySelf||(this._parent?.updateValueAndValidity(t),t.skipPristineCheck||this._parent?._updatePristine({},n),this._parent?._updateTouched({},n))}setParent(t){this._parent=t}getRawValue(){return this.value}updateValueAndValidity(t={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let r=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===fo||this.status===ur)&&this._runAsyncValidator(r,t.emitEvent)}let n=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Vs(this.value,n)),this._events.next(new fr(this.status,n)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.updateValueAndValidity(N(I({},t),{sourceControl:n}))}_updateTreeValidity(t={emitEvent:!0}){this._forEachChild(n=>n._updateTreeValidity(t)),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?mo:fo}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(t,n){if(this.asyncValidator){this.status=ur,this._hasOwnPendingAsyncValidator={emitEvent:n!==!1,shouldHaveEmitted:t!==!1};let r=jh(this.asyncValidator(this));this._asyncValidationSubscription=r.subscribe(o=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(o,{emitEvent:n,shouldHaveEmitted:t})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let t=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,t}return!1}setErrors(t,n={}){this.errors=t,this._updateControlsErrors(n.emitEvent!==!1,this,n.shouldHaveEmitted)}get(t){let n=t;return n==null||(Array.isArray(n)||(n=n.split(".")),n.length===0)?null:n.reduce((r,o)=>r&&r._find(o),this)}getError(t,n){let r=n?this.get(n):this;return r?.errors?r.errors[t]:null}hasError(t,n){return!!this.getError(t,n)}get root(){let t=this;for(;t._parent;)t=t._parent;return t}_updateControlsErrors(t,n,r){this.status=this._calculateStatus(),t&&this.statusChanges.emit(this.status),(t||r)&&this._events.next(new fr(this.status,n)),this._parent&&this._parent._updateControlsErrors(t,n,r)}_initObservables(){this.valueChanges=new le,this.statusChanges=new le}_calculateStatus(){return this._allControlsDisabled()?mo:this.errors?Os:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ur)?ur:this._anyControlsHaveStatus(Os)?Os:fo}_anyControlsHaveStatus(t){return this._anyControls(n=>n.status===t)}_anyControlsDirty(){return this._anyControls(t=>t.dirty)}_anyControlsTouched(){return this._anyControls(t=>t.touched)}_updatePristine(t,n){let r=!this._anyControlsDirty(),o=this.pristine!==r;this.pristine=r,t.onlySelf||this._parent?._updatePristine(t,n),o&&this._events.next(new po(this.pristine,n))}_updateTouched(t={},n){this.touched=this._anyControlsTouched(),this._events.next(new ho(this.touched,n)),t.onlySelf||this._parent?._updateTouched(t,n)}_onDisabledChange=[];_registerOnCollectionChange(t){this._onCollectionChange=t}_setUpdateStrategy(t){Gs(t)&&t.updateOn!=null&&(this._updateOn=t.updateOn)}_parentMarkedDirty(t){return!t&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(t){return null}_assignValidators(t){this._rawValidators=Array.isArray(t)?t.slice():t,this._composedValidatorFn=cC(this._rawValidators)}_assignAsyncValidators(t){this._rawAsyncValidators=Array.isArray(t)?t.slice():t,this._composedAsyncValidatorFn=lC(this._rawAsyncValidators)}},Hs=class extends Bs{constructor(t,n,r){super(Yh(n),Qh(r,n)),this.controls=t,this._initObservables(),this._setUpdateStrategy(n),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(t,n){return this.controls[t]?this.controls[t]:(this.controls[t]=n,n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange),n)}addControl(t,n,r={}){this.registerControl(t,n),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}removeControl(t,n={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],this.updateValueAndValidity({emitEvent:n.emitEvent}),this._onCollectionChange()}setControl(t,n,r={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],n&&this.registerControl(t,n),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}contains(t){return this.controls.hasOwnProperty(t)&&this.controls[t].enabled}setValue(t,n={}){uC(this,!0,t),Object.keys(t).forEach(r=>{dC(this,!0,r),this.controls[r].setValue(t[r],{onlySelf:!0,emitEvent:n.emitEvent})}),this.updateValueAndValidity(n)}patchValue(t,n={}){t!=null&&(Object.keys(t).forEach(r=>{let o=this.controls[r];o&&o.patchValue(t[r],{onlySelf:!0,emitEvent:n.emitEvent})}),this.updateValueAndValidity(n))}reset(t={},n={}){this._forEachChild((r,o)=>{r.reset(t?t[o]:null,N(I({},n),{onlySelf:!0}))}),this._updatePristine(n,this),this._updateTouched(n,this),this.updateValueAndValidity(n),n?.emitEvent!==!1&&this._events.next(new js(this))}getRawValue(){return this._reduceChildren({},(t,n,r)=>(t[r]=n.getRawValue(),t))}_syncPendingControls(){let t=this._reduceChildren(!1,(n,r)=>r._syncPendingControls()?!0:n);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){Object.keys(this.controls).forEach(n=>{let r=this.controls[n];r&&t(r,n)})}_setUpControls(){this._forEachChild(t=>{t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(t){for(let[n,r]of Object.entries(this.controls))if(this.contains(n)&&t(r))return!0;return!1}_reduceValue(){let t={};return this._reduceChildren(t,(n,r,o)=>((r.enabled||this.disabled)&&(n[o]=r.value),n))}_reduceChildren(t,n){let r=t;return this._forEachChild((o,i)=>{r=n(r,o,i)}),r}_allControlsDisabled(){for(let t of Object.keys(this.controls))if(this.controls[t].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(t){return this.controls.hasOwnProperty(t)?this.controls[t]:null}};var Kh=new g("",{factory:()=>wd}),wd="always";function fC(e,t){return[...t.path,e]}function Ah(e,t,n=wd){Id(e,t),t.valueAccessor.writeValue(e.value),(e.disabled||n==="always")&&t.valueAccessor.setDisabledState?.(e.disabled),pC(e,t),gC(e,t),hC(e,t),mC(e,t)}function Nh(e,t,n=!0){let r=()=>{};t?.valueAccessor?.registerOnChange(r),t?.valueAccessor?.registerOnTouched(r),$s(e,t),e&&(t._invokeOnDestroyCallbacks(),e._registerOnCollectionChange(()=>{}))}function Us(e,t){e.forEach(n=>{n.registerOnValidatorChange&&n.registerOnValidatorChange(t)})}function mC(e,t){if(t.valueAccessor.setDisabledState){let n=r=>{t.valueAccessor.setDisabledState(r)};e.registerOnDisabledChange(n),t._registerOnDestroy(()=>{e._unregisterOnDisabledChange(n)})}}function Id(e,t){let n=Gh(e);t.validator!==null?e.setValidators(Mh(n,t.validator)):typeof n=="function"&&e.setValidators([n]);let r=Wh(e);t.asyncValidator!==null?e.setAsyncValidators(Mh(r,t.asyncValidator)):typeof r=="function"&&e.setAsyncValidators([r]);let o=()=>e.updateValueAndValidity();Us(t._rawValidators,o),Us(t._rawAsyncValidators,o)}function $s(e,t){let n=!1;if(e!==null){if(t.validator!==null){let o=Gh(e);if(Array.isArray(o)&&o.length>0){let i=o.filter(s=>s!==t.validator);i.length!==o.length&&(n=!0,e.setValidators(i))}}if(t.asyncValidator!==null){let o=Wh(e);if(Array.isArray(o)&&o.length>0){let i=o.filter(s=>s!==t.asyncValidator);i.length!==o.length&&(n=!0,e.setAsyncValidators(i))}}}let r=()=>{};return Us(t._rawValidators,r),Us(t._rawAsyncValidators,r),n}function pC(e,t){t.valueAccessor.registerOnChange(n=>{e._pendingValue=n,e._pendingChange=!0,e._pendingDirty=!0,e.updateOn==="change"&&Xh(e,t)})}function hC(e,t){t.valueAccessor.registerOnTouched(()=>{e._pendingTouched=!0,e.updateOn==="blur"&&e._pendingChange&&Xh(e,t),e.updateOn!=="submit"&&e.markAsTouched()})}function Xh(e,t){e._pendingDirty&&e.markAsDirty(),e.setValue(e._pendingValue,{emitModelToViewChange:!1}),t.viewToModelUpdate(e._pendingValue),e._pendingChange=!1}function gC(e,t){let n=(r,o)=>{t.valueAccessor.writeValue(r),o&&t.viewToModelUpdate(r)};e.registerOnChange(n),t._registerOnDestroy(()=>{e._unregisterOnChange(n)})}function vC(e,t){e==null,Id(e,t)}function yC(e,t){return $s(e,t)}function bC(e,t){if(!e.hasOwnProperty("model"))return!1;let n=e.model;return n.isFirstChange()?!0:!Object.is(t,n.currentValue)}function _C(e){return Object.getPrototypeOf(e.constructor)===tC}function DC(e,t){e._syncPendingControls(),t.forEach(n=>{let r=n.control;r.updateOn==="submit"&&r._pendingChange&&(n.viewToModelUpdate(r._pendingValue),r._pendingChange=!1)})}function EC(e,t){if(!t)return null;Array.isArray(t);let n,r,o;return t.forEach(i=>{i.constructor===zs?n=i:_C(i)?r=i:o=i}),o||r||n||null}function CC(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function Rh(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function Fh(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var vo=class extends Bs{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(t=null,n,r){super(Yh(n),Qh(r,n)),this._applyFormState(t),this._setUpdateStrategy(n),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Gs(n)&&(n.nonNullable||n.initialValueIsDefault)&&(Fh(t)?this.defaultValue=t.value:this.defaultValue=t)}setValue(t,n={}){this.value=this._pendingValue=t,this._onChange.length&&n.emitModelToViewChange!==!1&&this._onChange.forEach(r=>r(this.value,n.emitViewToModelChange!==!1)),this.updateValueAndValidity(n)}patchValue(t,n={}){this.setValue(t,n)}reset(t=this.defaultValue,n={}){this._applyFormState(t),this.markAsPristine(n),this.markAsUntouched(n),this.setValue(this.value,n),n.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,n?.emitEvent!==!1&&this._events.next(new js(this))}_updateValue(){}_anyControls(t){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(t){this._onChange.push(t)}_unregisterOnChange(t){Rh(this._onChange,t)}registerOnDisabledChange(t){this._onDisabledChange.push(t)}_unregisterOnDisabledChange(t){Rh(this._onDisabledChange,t)}_forEachChild(t){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(t){Fh(t)?(this.value=this._pendingValue=t.value,t.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=t}};var wC=e=>e instanceof vo;var Jh=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return e})();var IC=(()=>{class e extends mr{callSetDisabledState;get submitted(){return at(this._submittedReactive)}set submitted(n){this._submittedReactive.set(n)}_submitted=ct(()=>this._submittedReactive());_submittedReactive=Ne(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(n,r,o){super(),this.callSetDisabledState=o,this._setValidators(n),this._setAsyncValidators(r)}ngOnChanges(n){this.onChanges(n)}ngOnDestroy(){this.onDestroy()}onChanges(n){this._checkFormPresent(),n.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&($s(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(n){let r=this.form.get(n.path);return Ah(r,n,this.callSetDisabledState),r.updateValueAndValidity({emitEvent:!1}),this.directives.push(n),r}getControl(n){return this.form.get(n.path)}removeControl(n){Nh(n.control||null,n,!1),CC(this.directives,n)}addFormGroup(n){this._setUpFormContainer(n)}removeFormGroup(n){this._cleanUpFormContainer(n)}getFormGroup(n){return this.form.get(n.path)}getFormArray(n){return this.form.get(n.path)}addFormArray(n){this._setUpFormContainer(n)}removeFormArray(n){this._cleanUpFormContainer(n)}updateModel(n,r){this.form.get(n.path).setValue(r)}onReset(){this.resetForm()}resetForm(n=void 0,r={}){this.form.reset(n,r),this._submittedReactive.set(!1)}onSubmit(n){return this.submitted=!0,DC(this.form,this.directives),this.ngSubmit.emit(n),this.form._events.next(new Cd(this.control)),n?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(n=>{let r=n.control,o=this.form.get(n.path);r!==o&&(Nh(r||null,n),wC(o)&&(Ah(o,n,this.callSetDisabledState),n.control=o))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(n){let r=this.form.get(n.path);vC(r,n),r.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(n){let r=this.form?.get(n.path);r&&yC(r,n)&&r.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Id(this.form,this),this._oldForm&&$s(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(r){return new(r||e)(j(Ph,10),j(Lh,10),j(Kh,8))};static \u0275dir=k({type:e,features:[Ie,bn]})}return e})();var eg=new g("");var xC={provide:go,useExisting:Mt(()=>xd)},xd=(()=>{class e extends go{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(n){}model;update=new le;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(n,r,o,i,s){super(),this._ngModelWarningConfig=s,this._parent=n,this._setValidators(r),this._setAsyncValidators(o),this.valueAccessor=EC(this,i)}ngOnChanges(n){this._added||this._setUpControl(),bC(n,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(n){this.viewModel=n,this.update.emit(n)}get path(){return fC(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(r){return new(r||e)(j(mr,13),j(Ph,10),j(Lh,10),j(kh,10),j(eg,8))};static \u0275dir=k({type:e,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[_t([xC]),Ie,bn]})}return e})();var MC={provide:mr,useExisting:Mt(()=>Md)},Md=(()=>{class e extends IC{form=null;ngSubmit=new le;get control(){return this.form}static \u0275fac=(()=>{let n;return function(o){return(n||(n=ts(e)))(o||e)}})();static \u0275dir=k({type:e,selectors:[["","formGroup",""]],hostBindings:function(r,o){r&1&&Fe("submit",function(s){return o.onSubmit(s)})("reset",function(){return o.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_t([MC]),Ie]})}return e})();var TC=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();var Ws=(()=>{class e{static withConfig(n){return{ngModule:e,providers:[{provide:eg,useValue:n.warnOnNgModelWithFormControl??"always"},{provide:Kh,useValue:n.callSetDisabledState??wd}]}}static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[TC]})}return e})();function zt(e){return e instanceof X?e.nativeElement:e}function tg(e){return e!=null&&`${e}`!="false"}var Td;try{Td=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Td=!1}var qe=(()=>{class e{_platformId=p(_n);isBrowser=this._platformId?ih(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Td)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Sd;function ng(){if(Sd==null){let e=typeof document<"u"?document.head:null;Sd=!!(e&&(e.createShadowRoot||e.attachShadow))}return Sd}function Ad(e){if(ng()){let t=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&t instanceof ShadowRoot)return t}return null}function Ze(e){return e.composedPath?e.composedPath()[0]:e.target}var yo;function rg(){if(yo==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>yo=!0}))}finally{yo=yo||!1}return yo}function pr(e){return rg()?e:!!e.capture}var qs=new WeakMap,bo=(()=>{class e{_appRef;_injector=p(ue);_environmentInjector=p(de);load(n){let r=this._appRef=this._appRef||this._injector.get(Cn),o=qs.get(r);o||(o={loaders:new Set,refs:[]},qs.set(r,o),r.onDestroy(()=>{qs.get(r)?.refs.forEach(i=>i.destroy()),qs.delete(r)})),o.loaders.has(n)||(o.loaders.add(n),o.refs.push(th(n,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var og=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();function _o(e){return e.buttons===0||e.detail===0}function Do(e){let t=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!t&&t.identifier===-1&&(t.radiusX==null||t.radiusX===1)&&(t.radiusY==null||t.radiusY===1)}var ig=new g("cdk-input-modality-detector-options"),sg={ignoreKeys:[18,17,224,91,16]},ag=650,Nd={passive:!0,capture:!0},cg=(()=>{class e{_platform=p(qe);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Xt(null);_options;_lastTouchMs=0;_onKeydown=n=>{this._options?.ignoreKeys?.some(r=>r===n.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Ze(n))};_onMousedown=n=>{Date.now()-this._lastTouchMs<ag||(this._modality.next(_o(n)?"keyboard":"mouse"),this._mostRecentTarget=Ze(n))};_onTouchstart=n=>{if(Do(n)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Ze(n)};constructor(){let n=p(R),r=p(Z),o=p(ig,{optional:!0});if(this._options=I(I({},sg),o),this.modalityDetected=this._modality.pipe(Ma(1)),this.modalityChanged=this.modalityDetected.pipe(Ca()),this._platform.isBrowser){let i=p(Ue).createRenderer(null,null);this._listenerCleanups=n.runOutsideAngular(()=>[i.listen(r,"keydown",this._onKeydown,Nd),i.listen(r,"mousedown",this._onMousedown,Nd),i.listen(r,"touchstart",this._onTouchstart,Nd)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(n=>n())}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Eo=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(Eo||{}),lg=new g("cdk-focus-monitor-default-options"),Zs=pr({passive:!0,capture:!0}),Rd=(()=>{class e{_ngZone=p(R);_platform=p(qe);_inputModalityDetector=p(cg);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=p(Z);_stopInputModalityDetector=new G;constructor(){let n=p(lg,{optional:!0});this._detectionMode=n?.detectionMode||Eo.IMMEDIATE}_rootNodeFocusAndBlurListener=n=>{let r=Ze(n);for(let o=r;o;o=o.parentElement)n.type==="focus"?this._onFocus(n,o):this._onBlur(n,o)};monitor(n,r=!1){let o=zt(n);if(!this._platform.isBrowser||o.nodeType!==1)return Ye();let i=Ad(o)||this._document,s=this._elementInfo.get(o);if(s)return r&&(s.checkChildren=!0),s.subject;let a={checkChildren:r,subject:new G,rootNode:i};return this._elementInfo.set(o,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(n){let r=zt(n),o=this._elementInfo.get(r);o&&(o.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(o))}focusVia(n,r,o){let i=zt(n),s=this._document.activeElement;i===s?this._getClosestElementsInfo(i).forEach(([a,c])=>this._originChanged(a,r,c)):(this._setOrigin(r),typeof i.focus=="function"&&i.focus(o))}ngOnDestroy(){this._elementInfo.forEach((n,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(n){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(n)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:n&&this._isLastInteractionFromInputLabel(n)?"mouse":"program"}_shouldBeAttributedToTouch(n){return this._detectionMode===Eo.EVENTUAL||!!n?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(n,r){n.classList.toggle("cdk-focused",!!r),n.classList.toggle("cdk-touch-focused",r==="touch"),n.classList.toggle("cdk-keyboard-focused",r==="keyboard"),n.classList.toggle("cdk-mouse-focused",r==="mouse"),n.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(n,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=n,this._originFromTouchInteraction=n==="touch"&&r,this._detectionMode===Eo.IMMEDIATE){clearTimeout(this._originTimeoutId);let o=this._originFromTouchInteraction?ag:1;this._originTimeoutId=setTimeout(()=>this._origin=null,o)}})}_onFocus(n,r){let o=this._elementInfo.get(r),i=Ze(n);!o||!o.checkChildren&&r!==i||this._originChanged(r,this._getFocusOrigin(i),o)}_onBlur(n,r){let o=this._elementInfo.get(r);!o||o.checkChildren&&n.relatedTarget instanceof Node&&r.contains(n.relatedTarget)||(this._setClasses(r),this._emitOrigin(o,null))}_emitOrigin(n,r){n.subject.observers.length&&this._ngZone.run(()=>n.subject.next(r))}_registerGlobalListeners(n){if(!this._platform.isBrowser)return;let r=n.rootNode,o=this._rootNodeFocusListenerCount.get(r)||0;o||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,Zs),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,Zs)}),this._rootNodeFocusListenerCount.set(r,o+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(en(this._stopInputModalityDetector)).subscribe(i=>{this._setOrigin(i,!0)}))}_removeGlobalListeners(n){let r=n.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let o=this._rootNodeFocusListenerCount.get(r);o>1?this._rootNodeFocusListenerCount.set(r,o-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Zs),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Zs),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(n,r,o){this._setClasses(n,r),this._emitOrigin(o,r),this._lastFocusOrigin=r}_getClosestElementsInfo(n){let r=[];return this._elementInfo.forEach((o,i)=>{(i===n||o.checkChildren&&i.contains(n))&&r.push([i,o])}),r}_isLastInteractionFromInputLabel(n){let{_mostRecentTarget:r,mostRecentModality:o}=this._inputModalityDetector;if(o!=="mouse"||!r||r===n||n.nodeName!=="INPUT"&&n.nodeName!=="TEXTAREA"||n.disabled)return!1;let i=n.labels;if(i){for(let s=0;s<i.length;s++)if(i[s].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var dg=new Set,xn,Fd=(()=>{class e{_platform=p(qe);_nonce=p(nr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):NC}matchMedia(n){return(this._platform.WEBKIT||this._platform.BLINK)&&AC(n,this._nonce),this._matchMedia(n)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function AC(e,t){if(!dg.has(e))try{xn||(xn=document.createElement("style"),t&&xn.setAttribute("nonce",t),xn.setAttribute("type","text/css"),document.head.appendChild(xn)),xn.sheet&&(xn.sheet.insertRule(`@media ${e} {body{ }}`,0),dg.add(e))}catch(n){console.error(n)}}function NC(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var RC=(()=>{class e{create(n){return typeof MutationObserver>"u"?null:new MutationObserver(n)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ug=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({providers:[RC]})}return e})();var Od={},hr=class e{_appId=p(tr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(t,n=!1){return this._appId!=="ng"&&(t+=this._appId),Od.hasOwnProperty(t)||(Od[t]=0),`${t}${n?e._infix+"-":""}${Od[t]++}`}static \u0275fac=function(n){return new(n||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})};var FC=new g("cdk-dir-doc",{providedIn:"root",factory:()=>p(Z)}),OC=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function fg(e){let t=e?.toLowerCase()||"";return t==="auto"&&typeof navigator<"u"&&navigator?.language?OC.test(navigator.language)?"rtl":"ltr":t==="rtl"?"rtl":"ltr"}var kd=(()=>{class e{get value(){return this.valueSignal()}valueSignal=Ne("ltr");change=new le;constructor(){let n=p(FC,{optional:!0});if(n){let r=n.body?n.body.dir:null,o=n.documentElement?n.documentElement.dir:null;this.valueSignal.set(fg(r||o||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var lt=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({})}return e})();var Pd=class{_box;_destroyed=new G;_resizeSubject=new G;_resizeObserver;_elementObservables=new Map;constructor(t){this._box=t,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(n=>this._resizeSubject.next(n)))}observe(t){return this._elementObservables.has(t)||this._elementObservables.set(t,new O(n=>{let r=this._resizeSubject.subscribe(n);return this._resizeObserver?.observe(t,{box:this._box}),()=>{this._resizeObserver?.unobserve(t),r.unsubscribe(),this._elementObservables.delete(t)}}).pipe(mt(n=>n.some(r=>r.target===t)),xa({bufferSize:1,refCount:!0}),en(this._destroyed))),this._elementObservables.get(t)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},mg=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=p(R);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,n]of this._observers)n.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(n,r){let o=r?.box||"content-box";return this._observers.has(o)||this._observers.set(o,new Pd(o)),this._observers.get(o).observe(n)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var kC=new g("MATERIAL_ANIMATIONS"),pg=null;function PC(){return p(kC,{optional:!0})?.animationsDisabled||p(vl,{optional:!0})==="NoopAnimations"?"di-disabled":(pg??=p(Fd).matchMedia("(prefers-reduced-motion)").matches,pg?"reduced-motion":"enabled")}function gr(){return PC()!=="enabled"}var LC=["notch"],VC=["matFormFieldNotchedOutline",""],jC=["*"],hg=["iconPrefixContainer"],gg=["textPrefixContainer"],vg=["iconSuffixContainer"],yg=["textSuffixContainer"],BC=["textField"],HC=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],UC=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function $C(e,t){e&1&&Re(0,"span",21)}function zC(e,t){if(e&1&&(B(0,"label",20),re(1,1),_e(2,$C,1,0,"span",21),H()),e&2){let n=Ge(2);ze("floating",n._shouldLabelFloat())("monitorResize",n._hasOutline())("id",n._labelId),st("for",n._control.disableAutomaticLabeling?null:n._control.id),Y(2),De(!n.hideRequiredMarker&&n._control.required?2:-1)}}function GC(e,t){if(e&1&&_e(0,zC,3,5,"label",20),e&2){let n=Ge();De(n._hasFloatingLabel()?0:-1)}}function WC(e,t){e&1&&Re(0,"div",7)}function qC(e,t){}function ZC(e,t){if(e&1&&jt(0,qC,0,0,"ng-template",13),e&2){Ge(2);let n=hs(1);ze("ngTemplateOutlet",n)}}function YC(e,t){if(e&1&&(B(0,"div",9),_e(1,ZC,1,1,null,13),H()),e&2){let n=Ge();ze("matFormFieldNotchedOutlineOpen",n._shouldLabelFloat()),Y(),De(n._forceDisplayInfixLabel()?-1:1)}}function QC(e,t){e&1&&(B(0,"div",10,2),re(2,2),H())}function KC(e,t){e&1&&(B(0,"div",11,3),re(2,3),H())}function XC(e,t){}function JC(e,t){if(e&1&&jt(0,XC,0,0,"ng-template",13),e&2){Ge();let n=hs(1);ze("ngTemplateOutlet",n)}}function ew(e,t){e&1&&(B(0,"div",14,4),re(2,4),H())}function tw(e,t){e&1&&(B(0,"div",15,5),re(2,5),H())}function nw(e,t){e&1&&Re(0,"div",16)}function rw(e,t){e&1&&(B(0,"div",18),re(1,6),H())}function ow(e,t){if(e&1&&(B(0,"mat-hint",22),xe(1),H()),e&2){let n=Ge(2);ze("id",n._hintLabelId),Y(),ir(n.hintLabel)}}function iw(e,t){if(e&1&&(B(0,"div",19),_e(1,ow,2,2,"mat-hint",22),re(2,7),Re(3,"div",23),re(4,8),H()),e&2){let n=Ge();Y(),De(n.hintLabel?1:-1)}}var Ld=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e,selectors:[["mat-label"]]})}return e})(),sw=new g("MatError");var Vd=(()=>{class e{align="start";id=p(hr).getId("mat-mdc-hint-");static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(r,o){r&2&&(Xr("id",o.id),st("align",null),J("mat-mdc-form-field-hint-end",o.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),aw=new g("MatPrefix");var cw=new g("MatSuffix");var Ig=new g("FloatingLabelParent"),bg=(()=>{class e{_elementRef=p(X);get floating(){return this._floating}set floating(n){this._floating=n,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(n){this._monitorResize=n,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=p(mg);_ngZone=p(R);_parent=p(Ig);_resizeSubscription=new te;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return lw(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(r,o){r&2&&J("mdc-floating-label--float-above",o.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function lw(e){let t=e;if(t.offsetParent!==null)return t.scrollWidth;let n=t.cloneNode(!0);n.style.setProperty("position","absolute"),n.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(n);let r=n.scrollWidth;return n.remove(),r}var _g="mdc-line-ripple--active",Ys="mdc-line-ripple--deactivating",Dg=(()=>{class e{_elementRef=p(X);_cleanupTransitionEnd;constructor(){let n=p(R),r=p(vt);n.runOutsideAngular(()=>{this._cleanupTransitionEnd=r.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let n=this._elementRef.nativeElement.classList;n.remove(Ys),n.add(_g)}deactivate(){this._elementRef.nativeElement.classList.add(Ys)}_handleTransitionEnd=n=>{let r=this._elementRef.nativeElement.classList,o=r.contains(Ys);n.propertyName==="opacity"&&o&&r.remove(_g,Ys)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),Eg=(()=>{class e{_elementRef=p(X);_ngZone=p(R);open=!1;_notch;ngAfterViewInit(){let n=this._elementRef.nativeElement,r=n.querySelector(".mdc-floating-label");r?(n.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(r.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>r.style.transitionDuration="")}))):n.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(n){let r=this._notch.nativeElement;!this.open||!n?r.style.width="":r.style.width=`calc(${n}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(n){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${n}px)`)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=se({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(r,o){if(r&1&&eo(LC,5),r&2){let i;ge(i=ve())&&(o._notch=i.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(r,o){r&2&&J("mdc-notched-outline--notched",o.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:VC,ngContentSelectors:jC,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(r,o){r&1&&(bt(),yt(0,"div",1),Bt(1,"div",2,0),re(3),Ht(),yt(4,"div",3))},encapsulation:2,changeDetection:0})}return e})(),dw=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e})}return e})();var uw=new g("MatFormField"),fw=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Cg="fill",mw="auto",wg="fixed",pw="translateY(-50%)",xg=(()=>{class e{_elementRef=p(X);_changeDetectorRef=p(ar);_platform=p(qe);_idGenerator=p(hr);_ngZone=p(R);_defaults=p(fw,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=no("iconPrefixContainer");_textPrefixContainerSignal=no("textPrefixContainer");_iconSuffixContainerSignal=no("iconSuffixContainer");_textSuffixContainerSignal=no("textSuffixContainer");_prefixSuffixContainers=ct(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(n=>n?.nativeElement).filter(n=>n!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Qp(Ld);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(n){this._hideRequiredMarker=tg(n)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||mw}set floatLabel(n){n!==this._floatLabel&&(this._floatLabel=n,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(n){let r=n||this._defaults?.appearance||Cg;this._appearanceSignal.set(r)}_appearanceSignal=Ne(Cg);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||wg}set subscriptSizing(n){this._subscriptSizing=n||this._defaults?.subscriptSizing||wg}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(n){this._hintLabel=n,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(n){this._explicitFormFieldControl=n}_destroyed=new G;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=gr();constructor(){let n=this._defaults,r=p(kd);n&&(n.appearance&&(this.appearance=n.appearance),this._hideRequiredMarker=!!n?.hideRequiredMarker,n.color&&(this.color=n.color)),Nc(()=>this._currentDirection=r.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=ct(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(n){let r=this._control,o="mat-mdc-form-field-type-";n&&this._elementRef.nativeElement.classList.remove(o+n.controlType),r.controlType&&this._elementRef.nativeElement.classList.add(o+r.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=r.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=r.stateChanges.pipe(Ta([void 0,void 0]),Q(()=>[r.errorState,r.userAriaDescribedBy]),wa(),mt(([[i,s],[a,c]])=>i!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),r.ngControl&&r.ngControl.valueChanges&&(this._valueChanges=r.ngControl.valueChanges.pipe(en(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(n=>!n._isText),this._hasTextPrefix=!!this._prefixChildren.find(n=>n._isText),this._hasIconSuffix=!!this._suffixChildren.find(n=>!n._isText),this._hasTextSuffix=!!this._suffixChildren.find(n=>n._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),ba(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let n=this._control.focused;n&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!n&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",n),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",n)}_syncOutlineLabelOffset(){eh({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let n of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(n,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:n=>this._writeOutlinedLabelStyles(n())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=ct(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(n){let r=this._control?this._control.ngControl:null;return r&&r[n]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let n=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&n.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let i=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;i?n.push(i.id):this._hintLabel&&n.push(this._hintLabelId),s&&n.push(s.id)}else this._errorChildren&&n.push(...this._errorChildren.map(i=>i.id));let r=this._control.describedByIds,o;if(r){let i=this._describedByIds||n;o=n.concat(r.filter(s=>s&&!i.includes(s)))}else o=n;this._control.setDescribedByIds(o),this._describedByIds=n}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let n=this._iconPrefixContainer?.nativeElement,r=this._textPrefixContainer?.nativeElement,o=this._iconSuffixContainer?.nativeElement,i=this._textSuffixContainer?.nativeElement,s=n?.getBoundingClientRect().width??0,a=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,l=i?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,h=`var(--mat-mdc-form-field-label-transform, ${pw} translateX(${f}))`,C=s+a+c+l;return[h,C]}_writeOutlinedLabelStyles(n){if(n!==null){let[r,o]=n;this._floatingLabel&&(this._floatingLabel.element.style.transform=r),o!==null&&this._notchedOutline?._setMaxWidth(o)}}_isAttachedToDom(){let n=this._elementRef.nativeElement;if(n.getRootNode){let r=n.getRootNode();return r&&r!==n}return document.documentElement.contains(n)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=se({type:e,selectors:[["mat-form-field"]],contentQueries:function(r,o,i){if(r&1&&(fs(i,o._labelChild,Ld,5),us(i,dw,5)(i,aw,5)(i,cw,5)(i,sw,5)(i,Vd,5)),r&2){ps();let s;ge(s=ve())&&(o._formFieldControl=s.first),ge(s=ve())&&(o._prefixChildren=s),ge(s=ve())&&(o._suffixChildren=s),ge(s=ve())&&(o._errorChildren=s),ge(s=ve())&&(o._hintChildren=s)}},viewQuery:function(r,o){if(r&1&&(ms(o._iconPrefixContainerSignal,hg,5)(o._textPrefixContainerSignal,gg,5)(o._iconSuffixContainerSignal,vg,5)(o._textSuffixContainerSignal,yg,5),eo(BC,5)(hg,5)(gg,5)(vg,5)(yg,5)(bg,5)(Eg,5)(Dg,5)),r&2){ps(4);let i;ge(i=ve())&&(o._textField=i.first),ge(i=ve())&&(o._iconPrefixContainer=i.first),ge(i=ve())&&(o._textPrefixContainer=i.first),ge(i=ve())&&(o._iconSuffixContainer=i.first),ge(i=ve())&&(o._textSuffixContainer=i.first),ge(i=ve())&&(o._floatingLabel=i.first),ge(i=ve())&&(o._notchedOutline=i.first),ge(i=ve())&&(o._lineRipple=i.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(r,o){r&2&&J("mat-mdc-form-field-label-always-float",o._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",o._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",o._hasIconSuffix)("mat-form-field-invalid",o._control.errorState)("mat-form-field-disabled",o._control.disabled)("mat-form-field-autofilled",o._control.autofilled)("mat-form-field-appearance-fill",o.appearance=="fill")("mat-form-field-appearance-outline",o.appearance=="outline")("mat-form-field-hide-placeholder",o._hasFloatingLabel()&&!o._shouldLabelFloat())("mat-primary",o.color!=="accent"&&o.color!=="warn")("mat-accent",o.color==="accent")("mat-warn",o.color==="warn")("ng-untouched",o._shouldForward("untouched"))("ng-touched",o._shouldForward("touched"))("ng-pristine",o._shouldForward("pristine"))("ng-dirty",o._shouldForward("dirty"))("ng-valid",o._shouldForward("valid"))("ng-invalid",o._shouldForward("invalid"))("ng-pending",o._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[_t([{provide:uw,useExisting:e},{provide:Ig,useExisting:e}])],ngContentSelectors:UC,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(r,o){if(r&1&&(bt(HC),jt(0,GC,1,1,"ng-template",null,0,Zl),B(2,"div",6,1),Fe("click",function(s){return o._control.onContainerClick(s)}),_e(4,WC,1,0,"div",7),B(5,"div",8),_e(6,YC,2,2,"div",9),_e(7,QC,3,0,"div",10),_e(8,KC,3,0,"div",11),B(9,"div",12),_e(10,JC,1,1,null,13),re(11),H(),_e(12,ew,3,0,"div",14),_e(13,tw,3,0,"div",15),H(),_e(14,nw,1,0,"div",16),H(),B(15,"div",17),_e(16,rw,2,0,"div",18)(17,iw,5,1,"div",19),H()),r&2){let i;Y(2),J("mdc-text-field--filled",!o._hasOutline())("mdc-text-field--outlined",o._hasOutline())("mdc-text-field--no-label",!o._hasFloatingLabel())("mdc-text-field--disabled",o._control.disabled)("mdc-text-field--invalid",o._control.errorState),Y(2),De(!o._hasOutline()&&!o._control.disabled?4:-1),Y(2),De(o._hasOutline()?6:-1),Y(),De(o._hasIconPrefix?7:-1),Y(),De(o._hasTextPrefix?8:-1),Y(2),De(!o._hasOutline()||o._forceDisplayInfixLabel()?10:-1),Y(2),De(o._hasTextSuffix?12:-1),Y(),De(o._hasIconSuffix?13:-1),Y(),De(o._hasOutline()?-1:14),Y(),J("mat-mdc-form-field-subscript-dynamic-size",o.subscriptSizing==="dynamic");let s=o._getSubscriptMessageType();Y(),De((i=s)==="error"?16:i==="hint"?17:-1)}},dependencies:[bg,Eg,od,Dg,Vd],styles:[`.mdc-text-field {
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
`],encapsulation:2,changeDetection:0})}return e})();var Co=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[ug,xg,lt]})}return e})();var Mg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[Co,Co,og,lt]})}return e})();var Oe=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(Oe||{}),jd=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Oe.HIDDEN;constructor(t,n,r,o=!1){this._renderer=t,this.element=n,this.config=r,this._animationForciblyDisabledThroughCss=o}fadeOut(){this._renderer.fadeOutRipple(this)}},Tg=pr({passive:!0,capture:!0}),Bd=class{_events=new Map;addHandler(t,n,r,o){let i=this._events.get(n);if(i){let s=i.get(r);s?s.add(o):i.set(r,new Set([o]))}else this._events.set(n,new Map([[r,new Set([o])]])),t.runOutsideAngular(()=>{document.addEventListener(n,this._delegateEventHandler,Tg)})}removeHandler(t,n,r){let o=this._events.get(t);if(!o)return;let i=o.get(n);i&&(i.delete(r),i.size===0&&o.delete(n),o.size===0&&(this._events.delete(t),document.removeEventListener(t,this._delegateEventHandler,Tg)))}_delegateEventHandler=t=>{let n=Ze(t);n&&this._events.get(t.type)?.forEach((r,o)=>{(o===n||o.contains(n))&&r.forEach(i=>i.handleEvent(t))})}},wo={enterDuration:225,exitDuration:150},hw=800,Sg=pr({passive:!0,capture:!0}),Ag=["mousedown","touchstart"],Ng=["mouseup","mouseleave","touchend","touchcancel"],gw=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=se({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,o){},styles:[`.mat-ripple {
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
`],encapsulation:2,changeDetection:0})}return e})(),Qs=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Bd;constructor(t,n,r,o,i){this._target=t,this._ngZone=n,this._platform=o,o.isBrowser&&(this._containerElement=zt(r)),i&&i.get(bo).load(gw)}fadeInRipple(t,n,r={}){let o=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),i=I(I({},wo),r.animation);r.centered&&(t=o.left+o.width/2,n=o.top+o.height/2);let s=r.radius||vw(t,n,o),a=t-o.left,c=n-o.top,l=i.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),m=u.transitionProperty,f=u.transitionDuration,h=m==="none"||f==="0s"||f==="0s, 0s"||o.width===0&&o.height===0,C=new jd(this,d,r,h);d.style.transform="scale3d(1, 1, 1)",C.state=Oe.FADING_IN,r.persistent||(this._mostRecentTransientRipple=C);let y=null;return!h&&(l||i.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let b=()=>{y&&(y.fallbackTimer=null),clearTimeout(dt),this._finishRippleTransition(C)},ee=()=>this._destroyRipple(C),dt=setTimeout(ee,l+100);d.addEventListener("transitionend",b),d.addEventListener("transitioncancel",ee),y={onTransitionEnd:b,onTransitionCancel:ee,fallbackTimer:dt}}),this._activeRipples.set(C,y),(h||!l)&&this._finishRippleTransition(C),C}fadeOutRipple(t){if(t.state===Oe.FADING_OUT||t.state===Oe.HIDDEN)return;let n=t.element,r=I(I({},wo),t.config.animation);n.style.transitionDuration=`${r.exitDuration}ms`,n.style.opacity="0",t.state=Oe.FADING_OUT,(t._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(t)}fadeOutAll(){this._getActiveRipples().forEach(t=>t.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(t=>{t.config.persistent||t.fadeOut()})}setupTriggerEvents(t){let n=zt(t);!this._platform.isBrowser||!n||n===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=n,Ag.forEach(r=>{e._eventManager.addHandler(this._ngZone,r,n,this)}))}handleEvent(t){t.type==="mousedown"?this._onMousedown(t):t.type==="touchstart"?this._onTouchStart(t):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Ng.forEach(n=>{this._triggerElement.addEventListener(n,this,Sg)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(t){t.state===Oe.FADING_IN?this._startFadeOutTransition(t):t.state===Oe.FADING_OUT&&this._destroyRipple(t)}_startFadeOutTransition(t){let n=t===this._mostRecentTransientRipple,{persistent:r}=t.config;t.state=Oe.VISIBLE,!r&&(!n||!this._isPointerDown)&&t.fadeOut()}_destroyRipple(t){let n=this._activeRipples.get(t)??null;this._activeRipples.delete(t),this._activeRipples.size||(this._containerRect=null),t===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),t.state=Oe.HIDDEN,n!==null&&(t.element.removeEventListener("transitionend",n.onTransitionEnd),t.element.removeEventListener("transitioncancel",n.onTransitionCancel),n.fallbackTimer!==null&&clearTimeout(n.fallbackTimer)),t.element.remove()}_onMousedown(t){let n=_o(t),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+hw;!this._target.rippleDisabled&&!n&&!r&&(this._isPointerDown=!0,this.fadeInRipple(t.clientX,t.clientY,this._target.rippleConfig))}_onTouchStart(t){if(!this._target.rippleDisabled&&!Do(t)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let n=t.changedTouches;if(n)for(let r=0;r<n.length;r++)this.fadeInRipple(n[r].clientX,n[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(t=>{let n=t.state===Oe.VISIBLE||t.config.terminateOnPointerUp&&t.state===Oe.FADING_IN;!t.config.persistent&&n&&t.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let t=this._triggerElement;t&&(Ag.forEach(n=>e._eventManager.removeHandler(n,t,this)),this._pointerUpEventsRegistered&&(Ng.forEach(n=>t.removeEventListener(n,this,Sg)),this._pointerUpEventsRegistered=!1))}};function vw(e,t,n){let r=Math.max(Math.abs(e-n.left),Math.abs(e-n.right)),o=Math.max(Math.abs(t-n.top),Math.abs(t-n.bottom));return Math.sqrt(r*r+o*o)}var Rg=new g("mat-ripple-global-options");var yw={capture:!0},bw=["focus","mousedown","mouseenter","touchstart"],Hd="mat-ripple-loader-uninitialized",Ud="mat-ripple-loader-class-name",Fg="mat-ripple-loader-centered",Ks="mat-ripple-loader-disabled",Og=(()=>{class e{_document=p(Z);_animationsDisabled=gr();_globalRippleOptions=p(Rg,{optional:!0});_platform=p(qe);_ngZone=p(R);_injector=p(ue);_eventCleanups;_hosts=new Map;constructor(){let n=p(Ue).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>bw.map(r=>n.listen(this._document,r,this._onInteraction,yw)))}ngOnDestroy(){let n=this._hosts.keys();for(let r of n)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(n,r){n.setAttribute(Hd,this._globalRippleOptions?.namespace??""),(r.className||!n.hasAttribute(Ud))&&n.setAttribute(Ud,r.className||""),r.centered&&n.setAttribute(Fg,""),r.disabled&&n.setAttribute(Ks,"")}setDisabled(n,r){let o=this._hosts.get(n);o?(o.target.rippleDisabled=r,!r&&!o.hasSetUpEvents&&(o.hasSetUpEvents=!0,o.renderer.setupTriggerEvents(n))):r?n.setAttribute(Ks,""):n.removeAttribute(Ks)}_onInteraction=n=>{let r=Ze(n);if(r instanceof HTMLElement){let o=r.closest(`[${Hd}="${this._globalRippleOptions?.namespace??""}"]`);o&&this._createRipple(o)}};_createRipple(n){if(!this._document||this._hosts.has(n))return;n.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",n.getAttribute(Ud)),n.append(r);let o=this._globalRippleOptions,i=this._animationsDisabled?0:o?.animation?.enterDuration??wo.enterDuration,s=this._animationsDisabled?0:o?.animation?.exitDuration??wo.exitDuration,a={rippleDisabled:this._animationsDisabled||o?.disabled||n.hasAttribute(Ks),rippleConfig:{centered:n.hasAttribute(Fg),terminateOnPointerUp:o?.terminateOnPointerUp,animation:{enterDuration:i,exitDuration:s}}},c=new Qs(a,this._ngZone,r,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(n),this._hosts.set(n,{target:a,renderer:c,hasSetUpEvents:l}),n.removeAttribute(Hd)}destroyRipple(n){let r=this._hosts.get(n);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(n))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var kg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=se({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,o){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2,changeDetection:0})}return e})();var _w=new g("MAT_BUTTON_CONFIG");function Pg(e){return e==null?void 0:Xp(e)}var Lg=(()=>{class e{_elementRef=p(X);_ngZone=p(R);_animationsDisabled=gr();_config=p(_w,{optional:!0});_focusMonitor=p(Rd);_cleanupClick;_renderer=p(vt);_rippleLoader=p(Og);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(n){this._disableRipple=n,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(n){this._disabled=n,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(n){this.tabIndex=n}constructor(){p(bo).load(kg);let n=this._elementRef.nativeElement;this._isAnchor=n.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(n,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(n="program",r){n?this._focusMonitor.focusVia(this._elementRef.nativeElement,n,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",n=>{this.disabled&&(n.preventDefault(),n.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=k({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(r,o){r&2&&(st("disabled",o._getDisabledAttribute())("aria-disabled",o._getAriaDisabled())("tabindex",o._getTabIndex()),ql(o.color?"mat-"+o.color:""),J("mat-mdc-button-disabled",o.disabled)("mat-mdc-button-disabled-interactive",o.disabledInteractive)("mat-unthemed",!o.color)("_mat-animation-noopable",o._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",cr],disabled:[2,"disabled","disabled",cr],ariaDisabled:[2,"aria-disabled","ariaDisabled",cr],disabledInteractive:[2,"disabledInteractive","disabledInteractive",cr],tabIndex:[2,"tabIndex","tabIndex",Pg],_tabindex:[2,"tabindex","_tabindex",Pg]}})}return e})();var Vg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[lt]})}return e})();var Dw=["matButton",""],Ew=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],Cw=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var jg=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Bg=(()=>{class e extends Lg{get appearance(){return this._appearance}set appearance(n){this.setAppearance(n||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let n=ww(this._elementRef.nativeElement);n&&this.setAppearance(n)}setAppearance(n){if(n===this._appearance)return;let r=this._elementRef.nativeElement.classList,o=this._appearance?jg.get(this._appearance):null,i=jg.get(n);o&&r.remove(...o),r.add(...i),this._appearance=n}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=se({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Ie],attrs:Dw,ngContentSelectors:Cw,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(r,o){r&1&&(bt(Ew),yt(0,"span",0),re(1),Bt(2,"span",1),re(3,1),Ht(),re(4,2),yt(5,"span",2)(6,"span",3)),r&2&&J("mdc-button__ripple",!o._isFab)("mdc-fab__ripple",o._isFab)},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2,changeDetection:0})}return e})();function ww(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var Hg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[Vg,lt]})}return e})();var xw=["*"];var Mw=new g("MAT_CARD_CONFIG"),Ug=(()=>{class e{appearance;constructor(){let n=p(Mw,{optional:!0});this.appearance=n?.appearance||"raised"}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=se({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(r,o){r&2&&J("mat-mdc-card-outlined",o.appearance==="outlined")("mdc-card--outlined",o.appearance==="outlined")("mat-mdc-card-filled",o.appearance==="filled")("mdc-card--filled",o.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:xw,decls:1,vars:0,template:function(r,o){r&1&&(bt(),re(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2,changeDetection:0})}return e})();var $g=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=$({type:e});static \u0275inj=U({imports:[lt]})}return e})();var Xs=class e{animals=[{id:1,name:"Lion",species:"Big Cat",healthStatus:"Healthy"},{id:2,name:"Elephant",species:"Mammal",healthStatus:"Good"}];getAnimals(){return Ye(this.animals).pipe(Ir(300),Q(t=>t))}addAnimal(t){let n=N(I({},t),{id:Date.now()});return this.animals.push(n),Ye(n).pipe(Ir(200))}deleteAnimal(t){return this.animals=this.animals.filter(n=>n.id!==t),Ye(void 0).pipe(Ir(200))}static \u0275fac=function(n){return new(n||e)};static \u0275prov=v({token:e,factory:e.\u0275fac,providedIn:"root"})};function Aw(e,t){if(e&1){let n=Wl();B(0,"mat-card",6)(1,"h3"),xe(2),H(),B(3,"p")(4,"strong"),xe(5,"Species:"),H(),xe(6),H(),B(7,"p")(8,"strong"),xe(9,"Health Status:"),H(),xe(10),H(),B(11,"button",7),Fe("click",function(){let o=Mi(n).$implicit,i=Ge();return Ti(i.removeAnimal(o.id))}),xe(12," Remove "),H()()}if(e&2){let n=t.$implicit;Y(2),ir(n.name),Y(4),sr(" ",n.species),Y(4),sr(" ",n.healthStatus)}}var Js=class e{constructor(t,n){this.animalService=t;this.cdr=n}animalForm=new Hs({name:new vo(""),species:new vo(""),healthStatus:new vo("")});animals=[];ngOnInit(){this.loadAnimals()}loadAnimals(){this.animalService.getAnimals().subscribe({next:t=>{this.animals=t,this.cdr.detectChanges()},error:t=>console.error(t)})}onSubmit(){if(this.animalForm.valid){let t=this.animalForm.value;this.animalService.addAnimal(t).subscribe({next:n=>{this.animals.push(n),this.animalForm.reset(),this.cdr.detectChanges()}})}}removeAnimal(t){this.animalService.deleteAnimal(t).subscribe({next:()=>{this.animals=this.animals.filter(n=>n.id!==t),this.cdr.detectChanges()}})}static \u0275fac=function(n){return new(n||e)(j(Xs),j(ar))};static \u0275cmp=se({type:e,selectors:[["app-add-animal"]],decls:8,vars:2,consts:[[3,"ngSubmit","formGroup"],["type","text","formControlName","name","placeholder","Name"],["type","text","formControlName","species","placeholder","Species"],["type","text","formControlName","healthStatus","placeholder","Health Status"],["mat-raised-button","","color","primary","type","submit"],["style","margin-top: 10px;",4,"ngFor","ngForOf"],[2,"margin-top","10px"],["mat-raised-button","","color","warn",3,"click"]],template:function(n,r){n&1&&(B(0,"form",0),Fe("ngSubmit",function(){return r.onSubmit()}),Re(1,"input",1)(2,"input",2)(3,"input",3),B(4,"button",4),xe(5," Add Animal "),H()(),Re(6,"hr"),jt(7,Aw,13,3,"mat-card",5)),n&2&&(ze("formGroup",r.animalForm),Y(7),ze("ngForOf",r.animals))},dependencies:[Es,Ds,Ws,Jh,zs,qh,Zh,Md,xd,Mg,Hg,Bg,$g,Ug,Co],encapsulation:2})};var ea=class e{static \u0275fac=function(n){return new(n||e)};static \u0275cmp=se({type:e,selectors:[["app-root"]],decls:3,vars:0,template:function(n,r){n&1&&(B(0,"h1"),xe(1,"Zoo Animal Management"),H(),Re(2,"app-add-animal"))},dependencies:[Js],encapsulation:2})};md(ea,{providers:[Ei(Dd,Ws)]}).catch(e=>console.error(e));
