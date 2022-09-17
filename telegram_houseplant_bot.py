#!/usr/bin/env python
# pylint: disable=unused-argument, wrong-import-position
# This program is dedicated to the public domain under the CC0 license.

"""
Simple Bot to reply to Telegram messages.

First, a few handler functions are defined. Then, those functions are passed to
the Application and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.
"""

import logging
import yaml
import time
import json
import uuid
import pprint

from telegram import __version__ as TG_VER
try:
    from telegram import __version_info__
except ImportError:
    __version_info__ = (0, 0, 0, 0, 0)  # type: ignore[assignment]

if __version_info__ < (20, 0, 0, "alpha", 1):
    raise RuntimeError(
        f"This example is not compatible with your current PTB version {TG_VER}. To view the "
        f"{TG_VER} version of this example, "
        f"visit https://docs.python-telegram-bot.org/en/v{TG_VER}/examples.html"
    )
from telegram import ForceReply, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (   
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import avro_helper

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

def fetch_configs():
    # fetches the configs from the available file
    with open(CONFIGS_FILE, 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.CLoader)

        return config

CONFIGS_FILE = './configs/configs.yaml'
CONFIGS = fetch_configs()

ID,GIVEN,SCIENTIFIC,COMMON,TEMP_LOW,TEMP_HIGH,MOISTURE_LOW,MOISTURE_HIGH,COMMIT_PLANT = range(9)
SENSOR_ID,PLANT_ID,COMMIT_MAPPING = range(3)

READINGS_TOPIC = 'houseplant-readings'
METADATA_TOPIC = 'houseplant-metadata'
MAPPING_TOPIC  = 'houseplant-sensor-mapping'


####################################################################################
#                                                                                  #
#                              UPDATE PLANT HANDLERS                               #
#                                                                                  #
####################################################################################

async def update_plant_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant'] = {}
        await update.message.reply_text(
            "Enter the plant id of the plant you'd like to update."
        )

        return ID
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END

async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['plant_id'] = int(update.message.text)

        await update.message.reply_text(
                 "Please enter plant's given name."
             )

        return GIVEN
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def given_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['given_name'] = update.message.text

        await update.message.reply_text(
                 "Please enter plant's scientific name."
             )

        return SCIENTIFIC
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def scientific_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['scientific_name'] = update.message.text

        await update.message.reply_text(
                 "Please enter plant's common name."
             )

        return COMMON
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def common_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['common_name'] = update.message.text

        await update.message.reply_text(
                 "Please enter plant's low temperature threshold in C or /skip to use the default."
             )

        return TEMP_LOW
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def temp_low_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        # capture and store low temp data
        temp_low = update.message.text
        if temp_low != '/skip':
            # update state
            context.user_data['plant']['temperature_low'] = int(temp_low)
        else:
            context.user_data['plant']['temperature_low'] = 40

        await update.message.reply_text(
                 "Please enter plant's high temperature threshold in C or /skip to use the default."
             )

        return TEMP_HIGH
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def temp_high_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        # capture and store high temp data
        temp_high = update.message.text
        if temp_high != '/skip':
            # update state
            context.user_data['plant']['temperature_high'] = int(temp_high)
        else:
            context.user_data['plant']['temperature_high'] = 100

        await update.message.reply_text(
                 "Please enter plant's low moisture threshold or /skip to use the default."
             )

        return MOISTURE_LOW
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def moisture_low_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        # capture and store high temp data
        moisture_low = update.message.text
        if moisture_low != '/skip':
            # update state
            context.user_data['plant']['moisture_low'] = int(moisture_low)
        else:
            context.user_data['plant']['moisture_low'] = 25

        await update.message.reply_text(
                 "Please enter plant's high moisture threshold or /skip to use the default."
             )

        return MOISTURE_HIGH
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def moisture_high_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        # capture and store high temp data
        moisture_high = update.message.text
        if moisture_high != '/skip':
            # update state
            context.user_data['plant']['moisture_high'] = int(moisture_high)
        else:
            context.user_data['plant']['moisture_high'] = 65

        # build up summary for confirmation
        summary = pprint.pformat(context.user_data['plant'])
        await update.message.reply_text(
            "Please confirm your metadata entry.\n\n"
            f"{summary}"
            "\nIs this correct? Reply /y to commmit the metadata entry or use /cancel to start over.", 
        )

        return COMMIT_PLANT
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def commit_plant_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        result = send_metadata(context.user_data['plant'])

        context.user_data['plant'].clear()
        if result == 0:
            await update.message.reply_text(
                "You've confirmed your metadata entry, and it has been udpated. "
                "Use /results see the results."
            )
        else:
            await update.message.reply_text(
                "Your metadata was not sent; please use /update_plant to re-try."
            )

        return ConversationHandler.END
    else:
        await update.message.reply_text(
                "You are you authorized to use this application."
            )
        return ConversationHandler.END


####################################################################################
#                                                                                  #
#                             UPDATE MAPPING HANDLERS                              #
#                                                                                  #
####################################################################################

async def update_mapping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['mapping'] = {}

        reply_keyboard = [
            ["0x36"], 
            ["0x37"], 
            ["0x38"], 
            ["0x39"]
        ]

        await update.message.reply_text(
            "Select the sensor ID for the mapping you'd like to update.",
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True, input_field_placeholder="Sensor ID"
            )
        )

        return SENSOR_ID
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def sensor_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['mapping']['sensor_id'] = update.message.text

        await update.message.reply_text(
            f"Please enter the plant id you'd like to map to {update.message.text}.", 
            reply_markup=ReplyKeyboardRemove()
        )

        return PLANT_ID
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def plant_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['mapping']['plant_id'] = int(update.message.text)

        # build up summary for confirmation
        summary = pprint.pformat(context.user_data['mapping'])
        await update.message.reply_text(
            "Please confirm your mapping entry.\n\n"
            f"{summary}"
            "\nIs this correct? Reply /y to commmit the mapping entry or use /cancel to start over.", 
        )

        return COMMIT_MAPPING
    else:
        await update.message.reply_text(
            "You are you authorized to use this application."
        )
        return ConversationHandler.END


async def commit_mapping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if CONFIGS['telegram']['chat-id'] == update.message.chat_id:
        result = send_mapping(context.user_data['mapping'])
        context.user_data['mapping'].clear()

        if result == 0:
            await update.message.reply_text(
                "You've confirmed your mapping entry, and it has been udpated. "
                "Use /results see the results."
            )
        else:
            await update.message.reply_text(
                "Your mapping entry was not sent; please use /update_mapping to re-try."
            )

        return ConversationHandler.END
    else:
        await update.message.reply_text(
                "You are you authorized to use this application."
            )
        return ConversationHandler.END


async def plants_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"Fetching houseplant metadata from Kafka..."
    )


async def latest_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"Fetching latest readings from Kafka..."
    )


async def mappings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"Fetching latest sensor-plant mappings from Kafka..."
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    context.user_data.clear()

    await update.message.reply_text(
        "Cancelled metadata update."
    )

    return ConversationHandler.END


def send_metadata(metadata): 
    # 1. set up schema registry
    sr_conf = {
        'url': CONFIGS['schema-registry']['schema.registry.url'],
        'basic.auth.user.info': CONFIGS['schema-registry']['basic.auth.user.info']
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # 2. set up metadata producer
    avro_serializer = AvroSerializer(
            schema_registry_client = schema_registry_client,
            schema_str = avro_helper.houseplant_schema,
            to_dict = avro_helper.Houseplant.houseplant_to_dict
    )

    producer_conf = CONFIGS['kafka']
    producer_conf['value.serializer'] = avro_serializer
    producer = SerializingProducer(producer_conf)

    # 3. send metadata message
    try:
        value = avro_helper.Houseplant.dict_to_houseplant(metadata)

        k = str(metadata.get('plant_id'))
        logger.info('Publishing metadata message for key ' + str(k))
        producer.produce(METADATA_TOPIC, key=k, value=value) 
        producer.poll()
        producer.flush()
    except Exception as e:
        print(str(e))
        logger.error('Got exception ' + str(e))
        return 1

    return 0


def send_mapping(mapping): 
    # 1. set up schema registry
    sr_conf = {
        'url': CONFIGS['schema-registry']['schema.registry.url'],
        'basic.auth.user.info': CONFIGS['schema-registry']['basic.auth.user.info']
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # 2. set up metadata producer
    avro_serializer = AvroSerializer(
            schema_registry_client = schema_registry_client,
            schema_str = avro_helper.mapping_schema,
            to_dict = avro_helper.Mapping.mapping_to_dict
    )

    producer_conf = CONFIGS['kafka']
    producer_conf['value.serializer'] = avro_serializer
    producer = SerializingProducer(producer_conf)

    # 3. send metadata message
    try:
        value = avro_helper.Mapping.dict_to_mapping(mapping)

        k = str(mapping.get('sensor_id'))
        logger.info('Publishing mapping message for key ' + str(k))
        producer.produce(MAPPING_TOPIC, key=k, value=value) 
        producer.poll()
        producer.flush()
    except Exception as e:
        print(str(e))
        logger.error('Got exception ' + str(e))
        return 1

    return 0


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands([
        ('latest', 'See latest readings'),
        ('plants', 'See all plants'),
        ('update_plant', 'Update plant metadata'),
        ('mappings', 'See sensor mappings'),
        ('update_mapping', 'Update sensor-plant mapping')
        ])


def main() -> None:
    # create the application and pass in bot token
    application = Application.builder().token(CONFIGS['telegram']['api-token']).post_init(post_init).build()

    # define conversation handlers
    update_plant_handler = ConversationHandler(
        entry_points=[CommandHandler("update_plant", update_plant_command)],
        states={
            ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, id_command),
                CommandHandler("cancel", cancel_command)
            ],
            GIVEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, given_command),
                CommandHandler("cancel", cancel_command)
            ],
            SCIENTIFIC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, scientific_command),
                CommandHandler("cancel", cancel_command)
            ],
            COMMON: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, common_command),
                CommandHandler("cancel", cancel_command)
            ],
            TEMP_LOW: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), temp_low_command), 
                CommandHandler("cancel", cancel_command)
            ],
            TEMP_HIGH: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), temp_high_command), 
                CommandHandler("cancel", cancel_command)
            ],
            MOISTURE_LOW: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), moisture_low_command), 
                CommandHandler("cancel", cancel_command)
            ],
            MOISTURE_HIGH: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), moisture_high_command), 
                CommandHandler("cancel", cancel_command)
            ],
            COMMIT_PLANT: [
                CommandHandler("y", commit_plant_command), 
                CommandHandler("n", cancel_command)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_command)]
    )

    update_mapping_handler = ConversationHandler(
        entry_points=[CommandHandler("update_mapping", update_mapping_command)],
        states={
            SENSOR_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sensor_id_command),
                CommandHandler("cancel", cancel_command)
            ],
            PLANT_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, plant_id_command),
                CommandHandler("cancel", cancel_command)
            ],
            COMMIT_MAPPING: [
                CommandHandler("y", commit_mapping_command), 
                CommandHandler("n", cancel_command)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_command)]
    )

    # add handlers
    application.add_handler(update_plant_handler)
    application.add_handler(update_mapping_handler)
    application.add_handler(CommandHandler("plants", plants_command))
    application.add_handler(CommandHandler("mappings", mappings_command))
    application.add_handler(CommandHandler("latest", latest_command))

    # run the bot application
    application.run_polling()


if __name__ == "__main__":
    main()
