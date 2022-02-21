package org.radarcns.consumer.realtime.action

import org.radarcns.config.ConfigRadar
import org.radarcns.config.realtime.ActionConfig
import java.io.IOException
import java.net.MalformedURLException
import javax.mail.internet.AddressException

/**
 * Factory class for [Action]s. It instantiates actions based on the configuration provided
 * for the given consumer.
 */
object ActionFactory {
    @JvmStatic
    fun getActionFor(config: ConfigRadar, actionConfig: ActionConfig): Action {
        when (actionConfig.name) {
            ActiveAppNotificationAction.NAME -> {
                return try {
                    ActiveAppNotificationAction(actionConfig)
                } catch (exc: MalformedURLException) {
                    throw IllegalArgumentException(
                            "The supplied url config was incorrect. Please check.", exc)
                }
            }
            EmailUserAction.NAME -> {
                return try {
                    EmailUserAction(actionConfig, config.emailServerConfig)
                } catch (e: AddressException) {
                    throw IllegalArgumentException("The configuration was invalid. Please check.", e)
                } catch (e: IOException) {
                    throw IllegalArgumentException("The configuration was invalid. Please check.", e)
                }
            }
            else -> throw IllegalArgumentException(
                    "The specified action with name " + actionConfig.name + " is " + "not correct.")
        }
    }
}