/*
 * Copyright (c) 2013-2015 Ashutosh Kumar Singh <me@aksingh.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.aksingh.owmjapis

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.header
import io.ktor.client.request.prepareRequest
import io.ktor.client.request.url
import io.ktor.http.HttpHeaders
import io.ktor.http.isSuccess
import kotlinx.coroutines.runBlocking
import org.json.JSONException
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.net.URLEncoder

/**
 *
 *
 * **The starting point for all API operations.**
 * If you're new to this API, read the docs for this class first.
 *
 *
 *
 * Lets you access data from OpenWeatherMap.org using its Weather APIs.
 * Henceforth, it's shortened as OWM.org to ease commenting.
 *
 *
 *
 * **Sample code:**<br></br>
 * `OpenWeatherMap.org owm = new OpenWeatherMap("your-api-key");`<br></br>
 * `OpenWeatherMap.org owm = new OpenWeatherMap(your-units, "your-api-key");`<br></br>
 * `OpenWeatherMap.org owm = new OpenWeatherMap(your-units, your-language, "your-api-key");`
 *
 *
 * @author Ashutosh Kumar Singh &lt;me@aksingh.net&gt;
 * @version 2015-01-17
 *
 * @param units  Any constant from Units
 * @param lang   Any constant from Language
 * @param apiKey API key from OWM.org
 *
 * @see [OpenWeatherMap.org](http://openweathermap.org/)
 *
 * @see [OpenWeatherMap.org API](http://openweathermap.org/api)
 * @see [OWM.org's Multilingual support](http://openweathermap.org/current.multi)
 *
 * @see [OWM.org's API Key](http://openweathermap.org/appid)
 * @since 2.5.0.1
 */
@Suppress("unused")
class OpenWeatherMap(units: String, lang: String, apiKey: String, client: HttpClient) {
    private val owmAddressInstance: OWMAddress = OWMAddress(units, lang, apiKey)
    private val owmResponse: OWMResponse = OWMResponse(client, owmAddressInstance)

    /**
     * Set API key for getting data from OWM.org
     * @see [OWM.org's API Key](http://openweathermap.org/appid)
     */
    var apiKey: String
        get() = owmAddressInstance.appId
        set(appId) {
            owmAddressInstance.appId = appId
        }

    /**
     * Set units for getting data from OWM.org
     */
    var units: String
        get() = owmAddressInstance.units
        set(units) {
            owmAddressInstance.units = units
        }
    val mode: String
        get() = owmAddressInstance.mode

    /**
     * Set language for getting data from OWM.org
     * @see [OWM.org's Multilingual support](http://openweathermap.org/current.multi)
     */
    var lang: String
        get() = owmAddressInstance.lang
        set(lang) {
            owmAddressInstance.lang = lang
        }

    @Throws(IOException::class, JSONException::class)
    fun currentWeatherByCityName(cityName: String?): CurrentWeather {
        val response = owmResponse.currentWeatherByCityName(cityName)
        return currentWeatherFromRawResponse(response)
    }

    @Throws(IOException::class, JSONException::class)
    fun currentWeatherByCityName(cityName: String, countryCode: String): CurrentWeather {
        val response = owmResponse.currentWeatherByCityName(cityName, countryCode)
        return currentWeatherFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun currentWeatherByCityCode(cityCode: Long): CurrentWeather {
        val response = owmResponse.currentWeatherByCityCode(cityCode)
        return currentWeatherFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun currentWeatherByCoordinates(latitude: Float, longitude: Float): CurrentWeather {
        val response = owmResponse.currentWeatherByCoordinates(latitude, longitude)
        return currentWeatherFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun currentWeatherFromRawResponse(response: String?): CurrentWeather {
        val jsonObj = response?.let { JSONObject(it) }
        return CurrentWeather(jsonObj)
    }

    @Throws(IOException::class, JSONException::class)
    fun hourlyForecastByCityName(cityName: String?): HourlyForecast {
        val response = owmResponse.hourlyForecastByCityName(cityName)
        return hourlyForecastFromRawResponse(response)
    }

    @Throws(IOException::class, JSONException::class)
    fun hourlyForecastByCityName(cityName: String, countryCode: String): HourlyForecast {
        val response = owmResponse.hourlyForecastByCityName(cityName, countryCode)
        return hourlyForecastFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun hourlyForecastByCityCode(cityCode: Long): HourlyForecast {
        val response = owmResponse.hourlyForecastByCityCode(cityCode)
        return hourlyForecastFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun hourlyForecastByCoordinates(latitude: Float, longitude: Float): HourlyForecast {
        val response = owmResponse.hourlyForecastByCoordinates(latitude, longitude)
        return hourlyForecastFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun hourlyForecastFromRawResponse(response: String?): HourlyForecast {
        val jsonObj = response?.let { JSONObject(it) }
        return HourlyForecast(jsonObj)
    }

    @Throws(IOException::class, JSONException::class)
    fun dailyForecastByCityName(cityName: String?, count: Byte): DailyForecast {
        val response = owmResponse.dailyForecastByCityName(cityName, count)
        return dailyForecastFromRawResponse(response)
    }

    @Throws(IOException::class, JSONException::class)
    fun dailyForecastByCityName(cityName: String, countryCode: String, count: Byte): DailyForecast {
        val response = owmResponse.dailyForecastByCityName(cityName, countryCode, count)
        return dailyForecastFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun dailyForecastByCityCode(cityCode: Long, count: Byte): DailyForecast {
        val response = owmResponse.dailyForecastByCityCode(cityCode, count)
        return dailyForecastFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun dailyForecastByCoordinates(latitude: Float, longitude: Float, count: Byte): DailyForecast {
        val response = owmResponse.dailyForecastByCoordinates(latitude, longitude, count)
        return dailyForecastFromRawResponse(response)
    }

    @Throws(JSONException::class)
    fun dailyForecastFromRawResponse(response: String?): DailyForecast {
        val jsonObj = response?.let { JSONObject(it) }
        return DailyForecast(jsonObj)
    }

    /**
     * Generates addresses for accessing the information from OWM.org
     *
     * @since 2.5.0.3
     */
    class OWMAddress(
        var units: String,
        var lang: String,
        var appId: String,
        val mode: String = MODE,
    ) {
        /*
        Addresses for current weather
         */
        @Throws(UnsupportedEncodingException::class)
        fun currentWeatherByCityName(cityName: String?): String {
            return URL_API + URL_CURRENT +
                    PARAM_CITY_NAME + URLEncoder.encode(cityName, ENCODING) + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        @Throws(UnsupportedEncodingException::class)
        fun currentWeatherByCityName(cityName: String, countryCode: String): String {
            return currentWeatherByCityName("$cityName,$countryCode")
        }

        fun currentWeatherByCityCode(cityCode: Long): String {
            return URL_API + URL_CURRENT +
                    PARAM_CITY_ID + cityCode.toString() + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        fun currentWeatherByCoordinates(latitude: Float, longitude: Float): String {
            return URL_API + URL_CURRENT +
                    PARAM_LATITUDE + latitude.toString() + "&" +
                    PARAM_LONGITUDE + longitude.toString() + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_APPID + appId
        }

        /*
        Addresses for hourly forecasts
         */
        @Throws(UnsupportedEncodingException::class)
        fun hourlyForecastByCityName(cityName: String?): String {
            return URL_API + URL_HOURLY_FORECAST +
                    PARAM_CITY_NAME + URLEncoder.encode(cityName, ENCODING) + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        @Throws(UnsupportedEncodingException::class)
        fun hourlyForecastByCityName(cityName: String, countryCode: String): String {
            return hourlyForecastByCityName("$cityName,$countryCode")
        }

        fun hourlyForecastByCityCode(cityCode: Long): String {
            return URL_API + URL_HOURLY_FORECAST +
                    PARAM_CITY_ID + java.lang.Long.toString(cityCode) + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        fun hourlyForecastByCoordinates(latitude: Float, longitude: Float): String {
            return URL_API + URL_HOURLY_FORECAST +
                    PARAM_LATITUDE + latitude.toString() + "&" +
                    PARAM_LONGITUDE + longitude.toString() + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        /*
        Addresses for daily forecasts
         */
        @Throws(UnsupportedEncodingException::class)
        fun dailyForecastByCityName(cityName: String?, count: Byte): String {
            return URL_API + URL_DAILY_FORECAST +
                    PARAM_CITY_NAME + URLEncoder.encode(cityName, ENCODING) + "&" +
                    PARAM_COUNT + count.toString() + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        @Throws(UnsupportedEncodingException::class)
        fun dailyForecastByCityName(cityName: String, countryCode: String, count: Byte): String {
            return dailyForecastByCityName("$cityName,$countryCode", count)
        }

        fun dailyForecastByCityCode(cityCode: Long, count: Byte): String {
            return URL_API + URL_DAILY_FORECAST +
                    PARAM_CITY_ID + cityCode.toString() + "&" +
                    PARAM_COUNT + count.toString() + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        fun dailyForecastByCoordinates(latitude: Float, longitude: Float, count: Byte): String {
            return URL_API + URL_DAILY_FORECAST +
                    PARAM_LATITUDE + latitude.toString() + "&" +
                    PARAM_LONGITUDE + longitude.toString() + "&" +
                    PARAM_COUNT + count.toString() + "&" +
                    PARAM_MODE + mode + "&" +
                    PARAM_UNITS + units + "&" +
                    PARAM_LANG + lang + "&" +
                    PARAM_APPID + appId
        }

        companion object {
            private const val MODE = "json"
            private const val ENCODING = "UTF-8"
        }
    }

    /**
     * Requests OWM.org for data and provides back the incoming response.
     *
     * @since 2.5.0.3
     */
    private class OWMResponse(
        private val client: HttpClient,
        private val owmAddress: OWMAddress,
    ) {
        /*
        Responses for current weather
         */
        @Throws(UnsupportedEncodingException::class)
        fun currentWeatherByCityName(cityName: String?): String? {
            val address = owmAddress.currentWeatherByCityName(cityName)
            return httpGet(address)
        }

        @Throws(UnsupportedEncodingException::class)
        fun currentWeatherByCityName(cityName: String, countryCode: String): String? {
            val address = owmAddress.currentWeatherByCityName(cityName, countryCode)
            return httpGet(address)
        }

        fun currentWeatherByCityCode(cityCode: Long): String? {
            val address = owmAddress.currentWeatherByCityCode(cityCode)
            return httpGet(address)
        }

        fun currentWeatherByCoordinates(latitude: Float, longitude: Float): String? {
            val address = owmAddress.currentWeatherByCoordinates(latitude, longitude)
            return httpGet(address)
        }

        /*
        Responses for hourly forecasts
         */
        @Throws(UnsupportedEncodingException::class)
        fun hourlyForecastByCityName(cityName: String?): String? {
            val address = owmAddress.hourlyForecastByCityName(cityName)
            return httpGet(address)
        }

        @Throws(UnsupportedEncodingException::class)
        fun hourlyForecastByCityName(cityName: String, countryCode: String): String? {
            val address = owmAddress.hourlyForecastByCityName(cityName, countryCode)
            return httpGet(address)
        }

        fun hourlyForecastByCityCode(cityCode: Long): String? {
            val address = owmAddress.hourlyForecastByCityCode(cityCode)
            return httpGet(address)
        }

        fun hourlyForecastByCoordinates(latitude: Float, longitude: Float): String? {
            val address = owmAddress.hourlyForecastByCoordinates(latitude, longitude)
            return httpGet(address)
        }

        /*
        Responses for daily forecasts
         */
        @Throws(UnsupportedEncodingException::class)
        fun dailyForecastByCityName(cityName: String?, count: Byte): String? {
            val address = owmAddress.dailyForecastByCityName(cityName, count)
            return httpGet(address)
        }

        @Throws(UnsupportedEncodingException::class)
        fun dailyForecastByCityName(cityName: String, countryCode: String, count: Byte): String? {
            val address = owmAddress.dailyForecastByCityName(cityName, countryCode, count)
            return httpGet(address)
        }

        fun dailyForecastByCityCode(cityCode: Long, count: Byte): String? {
            val address = owmAddress.dailyForecastByCityCode(cityCode, count)
            return httpGet(address)
        }

        fun dailyForecastByCoordinates(latitude: Float, longitude: Float, count: Byte): String? {
            val address = owmAddress.dailyForecastByCoordinates(latitude, longitude, count)
            return httpGet(address)
        }

        /**
         * Implements HTTP's GET method
         *
         * @param requestAddress Address to be loaded
         * @return Response if successful, else `null`
         * @see [HTTP -
        ](http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html) */
        private fun httpGet(requestAddress: String): String? = runBlocking{

            val request = client.prepareRequest {
                url(requestAddress)
                header(HttpHeaders.AcceptEncoding, "gzip, deflate")
                header(HttpHeaders.CacheControl, "no-cache")
                header(HttpHeaders.Pragma, "no-cache")
            }

            try {
                request.execute { response ->
                    val responseString = response.body<String>()
                    if (response.status.isSuccess() && responseString.isNotEmpty()) {
                        responseString
                    } else {
                        logger.error(
                            "Failed to request body (HTTP code {}): {}",
                            response.status.value,
                            responseString,
                        )
                        null
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to call API", e)
                null
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(OpenWeatherMap::class.java)

        /*
    URLs and parameters for OWM.org
     */
        const val URL_API = "http://api.openweathermap.org/data/2.5/"
        private const val URL_CURRENT = "weather?"
        private const val URL_HOURLY_FORECAST = "forecast?"
        private const val URL_DAILY_FORECAST = "forecast/daily?"
        private const val PARAM_COUNT = "cnt="
        private const val PARAM_CITY_NAME = "q="
        private const val PARAM_CITY_ID = "id="
        private const val PARAM_LATITUDE = "lat="
        private const val PARAM_LONGITUDE = "lon="
        private const val PARAM_MODE = "mode="
        private const val PARAM_UNITS = "units="
        private const val PARAM_APPID = "appId="
        private const val PARAM_LANG = "lang="

        /**
         * Languages that can be set for getting data from OWM.org
         *
         * @since 2.5.0.3
         */
        const val LANGUAGE_ENGLISH = "en"
        const val LANGUAGE_RUSSIAN = "ru"
        const val LANGUAGE_ITALIAN = "it"
        const val LANGUAGE_SPANISH = "es"
        const val LANGUAGE_UKRAINIAN = "uk"
        const val LANGUAGE_GERMAN = "de"
        const val LANGUAGE_PORTUGUESE = "pt"
        const val LANGUAGE_ROMANIAN = "ro"
        const val LANGUAGE_POLISH = "pl"
        const val LANGUAGE_FINNISH = "fi"
        const val LANGUAGE_DUTCH = "nl"
        const val LANGUAGE_FRENCH = "FR"
        const val LANGUAGE_BULGARIAN = "bg"
        const val LANGUAGE_SWEDISH = "sv"
        const val LANGUAGE_CHINESE_TRADITIONAL = "zh_tw"
        const val LANGUAGE_CHINESE_SIMPLIFIED = "zh"
        const val LANGUAGE_TURKISH = "tr"
        const val LANGUAGE_CROATIAN = "hr"
        const val LANGUAGE_CATALAN = "ca"

        /**
         * Units that can be set for getting data from OWM.org
         *
         * @since 2.5.0.3
         */
        const val UNITS_METRIC = "metric"
        const val UNITS_IMPERIAL = "imperial"
    }
}
