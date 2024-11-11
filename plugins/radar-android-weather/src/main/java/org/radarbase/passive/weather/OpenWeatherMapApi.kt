/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.passive.weather

import io.ktor.client.HttpClient
import net.aksingh.owmjapis.CurrentWeather
import net.aksingh.owmjapis.OpenWeatherMap
import org.json.JSONException
import org.radarcns.passive.weather.WeatherCondition
import java.io.IOException
import java.math.BigDecimal
import java.util.*

internal class OpenWeatherMapApi(apiKey: String, client: HttpClient) : WeatherApi {
    private val owm: OpenWeatherMap = OpenWeatherMap(OpenWeatherMap.UNITS_METRIC,
            OpenWeatherMap.LANGUAGE_ENGLISH, apiKey, client)

    override val sourceName = "OpenWeatherMap"

    @Throws(IOException::class)
    override fun loadCurrentWeather(latitude: Double, longitude: Double): WeatherApiResult {
        val cw: CurrentWeather = try {
            owm.currentWeatherByCoordinates(latitude.toFloat(), longitude.toFloat())
        } catch (ex: JSONException) {
            throw IOException("Could not parse weather data from the OpenWeatherMap API " +
                    "for latitude " + latitude + " and longitude " + longitude, ex)
        }

        return if (cw.isValid) {
            OpenWeatherMapApiResult(cw)
        } else {
            throw IOException("Could not get weather data from the OpenWeatherMap API " +
                    "for latitude " + latitude + " and longitude " + longitude)
        }
    }

    private class OpenWeatherMapApiResult(private val cw: CurrentWeather) : WeatherApiResult {
        override val timestamp: Double = System.currentTimeMillis() / 1000.0
        override val temperature: Float?
        override val sunSet: Int?
        override val pressure: Float?
        override val humidity: Float?
        override val precipitation: Float?
        override val precipitationPeriod: Int?
        override val sunRise: Int?

        init {
            val main = cw.mainInstance
            if (main != null) {
                temperature = if (main.hasTemperature) main.temperature else null
                pressure = if (main.hasPressure) main.pressure else null
                humidity = if (main.hasHumidity) main.humidity else null
            } else {
                temperature = null
                pressure = null
                humidity = null
            }

            val sys = cw.sysInstance
            if (sys != null) {
                sunRise = getTimeOfDayFromDate(sys.sunriseTime)
                sunSet = getTimeOfDayFromDate(sys.sunsetTime)
            } else {
                sunRise = null
                sunSet = null
            }

            precipitation = compute3hPrecipitation(cw.rainInstance, cw.snowInstance)
            precipitationPeriod = if (precipitation != null) 3 else null
        }

        override val cloudiness: Float?
            get() {
                val clouds = cw.cloudsInstance
                return if (clouds?.hasPercentageOfClouds() == true) clouds.percentageOfClouds else null
            }

        override val weatherCondition: WeatherCondition
            get() {
                if (!cw.hasWeatherInstance) {
                    return WeatherCondition.UNKNOWN
                }
                // Get weather code of primary weather condition instance
                val code = cw.getWeatherInstance(0).weatherCode
                return when {
                    code in 200..299 -> WeatherCondition.THUNDER
                    code in 300..399 -> WeatherCondition.DRIZZLE
                    code in 500..599 -> WeatherCondition.RAINY
                    code in 600..699 -> WeatherCondition.SNOWY
                    code == 701 || code == 721 || code == 741 -> WeatherCondition.FOGGY
                    code == 800 -> WeatherCondition.CLEAR
                    code in 801..899 -> WeatherCondition.CLOUDY
                    code == 900 || code == 901 || code == 902 || code == 905 || code >= 957 -> // tornado, tropical storm, hurricane, windy and high wind to hurricane
                        WeatherCondition.STORM
                    code == 906 -> // hail
                        WeatherCondition.ICY
                    else -> WeatherCondition.OTHER
                }
            }

        override fun toString() = cw.toString()
    }

    companion object {
        private fun compute3hPrecipitation(rain: CurrentWeather.Rain?, snow: CurrentWeather.Snow?): Float? {
            return if (rain != null || snow != null) {
                var totalPrecipitation = BigDecimal.ZERO
                if (rain?.hasRain3h() == true) {
                    totalPrecipitation = totalPrecipitation.add(BigDecimal(rain.rain3h.toString()))
                }

                if (snow?.hasSnow3h() == true) {
                    totalPrecipitation = totalPrecipitation.add(BigDecimal(snow.snow3h.toString()))
                }
                totalPrecipitation.toFloat()
            } else {
                null
            }
        }

        /**
         * Get the time of day in minutes precision from a date object
         * in the current time zone of the device.
         * @param date a date object
         * @return whole minutes from midnight in current timezone
         */
        private fun getTimeOfDayFromDate(date: Date?): Int? {
            if (date == null || date.time == 0L) {
                return null
            }

            return Calendar.getInstance(TimeZone.getDefault()).run {
                time = date
                get(Calendar.HOUR_OF_DAY) * 60 + get(Calendar.MINUTE)
            }
        }
    }
}
